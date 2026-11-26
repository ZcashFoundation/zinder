//! What the release gate frees, and what it must not delay.

use super::*;

/// Window the stalled client grants each stream, smaller than one message.
const SMALL_WINDOW: u32 = 64 * 1024;

#[tokio::test]
async fn a_stalled_stream_frees_its_data_while_its_producer_keeps_producing() -> Result<()> {
    let mut server = serve(Bounds::default())?;
    let channel = tonic_client(server.address, SMALL_WINDOW).await?;
    let mut stream = open_stream(&channel).await?;
    let mut script = server.next_script().await?;
    // The producer sends until the transport stops taking data, then waits.
    let producer = tokio::spawn(async move { while script.send_message(PAYLOAD).await.is_ok() {} });

    wait_until("every message the stalled stream holds is freed", || {
        server.all_freed(3)
    })
    .await?;
    within_wait_limit(producer).await??;
    let (_, status) = read_to_end(&mut stream).await?;
    assert_eq!(status, Code::Cancelled);
    Ok(())
}

#[tokio::test]
async fn a_paused_stream_whose_data_waits_for_window_is_reset() -> Result<()> {
    let mut server = serve(Bounds::default())?;
    let channel = tonic_client(server.address, SMALL_WINDOW).await?;
    let mut stream = open_stream(&channel).await?;
    let mut script = server.next_script().await?;
    // One message larger than the window, then nothing more: the producer
    // stays open with data still waiting for window.
    script.send_message(PAYLOAD).await?;

    wait_until("the paused stream's message is freed", || {
        server.all_freed(1)
    })
    .await?;
    let (_, status) = read_to_end(&mut stream).await?;
    assert_eq!(status, Code::Cancelled);
    drop(script);
    Ok(())
}

#[tokio::test]
async fn a_finished_stream_whose_tail_waits_for_window_is_reset() -> Result<()> {
    let mut server = serve(Bounds::default())?;
    let channel = tonic_client(server.address, SMALL_WINDOW).await?;
    let mut stream = open_stream(&channel).await?;
    let mut script = server.next_script().await?;
    // The end is queued in the same turn as the message, so the gate sees
    // it when it looks past the message.
    script.send_message(PAYLOAD).await?;
    script.finish().await?;

    // Without the gate, hyper would queue the end behind data the window
    // does not cover, and nothing could release that data or the stream.
    wait_until("the finished stream's message is freed", || {
        server.all_freed(1)
    })
    .await?;
    let (_, status) = read_to_end(&mut stream).await?;
    assert_eq!(status, Code::Cancelled);
    Ok(())
}

#[tokio::test]
async fn a_reader_that_grants_window_late_receives_every_byte_and_the_status() -> Result<()> {
    let mut server = serve(Bounds::default())?;
    let body = raw_request(server.address, 65_535, None).await?.into_body();
    let mut script = server.next_script().await?;
    for round in 0..6 {
        script.send_message(PAYLOAD + round * 7).await?;
        // A pause between messages makes the gate probe the window.
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let sent = script.sent.clone();
    script.finish().await?;

    // Reading starts after a pause shorter than the send stall timeout.
    tokio::time::sleep(STALL / 4).await;
    let (received, trailers) = raw_body(body).await?;
    assert!(received == sent, "the body's bytes changed on the way");
    let status = trailers
        .as_ref()
        .and_then(|trailers| trailers.get("grpc-status"));
    assert_eq!(
        status.map(http::HeaderValue::as_bytes),
        Some(b"0".as_slice())
    );
    Ok(())
}

#[tokio::test]
async fn a_slow_reader_is_never_reset() -> Result<()> {
    let mut server = serve(Bounds::default())?;
    let channel = tonic_client(server.address, SMALL_WINDOW).await?;
    let mut stream = open_stream(&channel).await?;
    let mut script = server.next_script().await?;
    let messages = 10;
    let producer = tokio::spawn(async move {
        for _ in 0..messages {
            script.send_message(PAYLOAD).await?;
        }
        script.finish().await
    });

    let mut received = 0;
    while let Some(message) = within_wait_limit(stream.next()).await? {
        message?;
        received += 1;
        // Each read comes well within the send stall timeout.
        tokio::time::sleep(STALL * 3 / 5).await;
    }
    assert_eq!(received, messages);
    within_wait_limit(producer).await???;
    Ok(())
}

#[tokio::test]
async fn a_paused_stream_delivers_what_it_has_without_waiting_for_more() -> Result<()> {
    let mut server = serve(Bounds::default())?;
    let channel = tonic_client(server.address, 16 * 1024 * 1024).await?;
    let mut stream = open_stream(&channel).await?;
    let mut script = server.next_script().await?;
    let started = Instant::now();
    script.send_message(1500).await?;

    let first = within_wait_limit(stream.next()).await?;
    assert!(
        matches!(first, Some(Ok(_))),
        "the paused stream's message did not arrive"
    );
    assert!(
        started.elapsed() < STALL,
        "the message waited {:?} for a later frame",
        started.elapsed()
    );
    drop(script);
    Ok(())
}

#[tokio::test]
async fn a_unary_response_arrives_without_waiting_for_more_frames() -> Result<()> {
    let mut server = serve(Bounds::default())?;
    let body = raw_request(server.address, 65_535, None).await?.into_body();
    let mut script = server.next_script().await?;
    let started = Instant::now();
    script.send_message(400).await?;
    let sent = script.sent.clone();
    script.finish().await?;

    let (received, trailers) = raw_body(body).await?;
    assert!(
        started.elapsed() < STALL,
        "the response waited {:?}",
        started.elapsed()
    );
    assert!(received == sent, "the body's bytes changed on the way");
    assert!(trailers.is_some_and(|trailers| trailers.contains_key("grpc-status")));
    Ok(())
}

/// Reads a 256 KiB response with a 64 KiB window, pacing its reads to read
/// half that window per `half_window_period`, and returns the bytes read,
/// the response's outcome, and the bytes the server sent.
async fn read_paced(
    half_window_period: Duration,
) -> Result<(Vec<u8>, Result<Option<http::HeaderMap>>, Vec<u8>)> {
    const WINDOW: u32 = 64 * 1024;
    const MESSAGES: usize = 16;
    const MESSAGE: usize = 16 * 1024;
    let mut server = serve(Bounds::default())?;
    let response = raw_request(server.address, WINDOW, None).await?;
    let mut script = server.next_script().await?;
    // A reset drops the handler, so a send fails once it happens.
    let producer = tokio::spawn(async move {
        for _ in 0..MESSAGES {
            if script.send_message(MESSAGE).await.is_err() {
                return script.sent;
            }
        }
        let sent = script.sent.clone();
        let _ = script.finish().await;
        sent
    });

    let mut body = response.into_body();
    let mut received = Vec::new();
    let mut trailers = None;
    let outcome = loop {
        let Some(frame) = within_wait_limit(body.frame()).await? else {
            break Ok(trailers);
        };
        let frame = match frame {
            Ok(frame) => frame,
            Err(error) => break Err(error.into()),
        };
        if let Some(chunk) = frame.data_ref() {
            received.extend_from_slice(chunk);
            let read = u32::try_from(chunk.len())?;
            tokio::time::sleep(half_window_period * read / (WINDOW / 2)).await;
        }
        if let Ok(frame_trailers) = frame.into_trailers() {
            trailers = Some(frame_trailers);
        }
    };
    let sent = within_wait_limit(producer).await??;
    Ok((received, outcome, sent))
}

/// A reader is reset only if HTTP/2 holds one piece for the whole timeout.
///
/// A waiting piece goes out once the window covers everything queued ahead of
/// it plus one byte. With 16 KiB messages and this reader's h2 window
/// updates, reading half its window within the timeout covers that, however
/// long the response takes.
#[tokio::test]
async fn a_reader_whose_window_updates_come_within_the_timeout_is_never_reset() -> Result<()> {
    let started = Instant::now();
    let (received, outcome, sent) = read_paced(STALL * 4 / 5).await?;
    assert!(
        started.elapsed() > STALL * 3,
        "the response took only {:?}, too short to show anything",
        started.elapsed()
    );
    assert_eq!(received, sent);
    let trailers = outcome?;
    let status = trailers
        .as_ref()
        .and_then(|trailers| trailers.get("grpc-status"));
    assert_eq!(
        status.map(http::HeaderValue::as_bytes),
        Some(b"0".as_slice())
    );
    Ok(())
}

#[tokio::test]
async fn a_reader_whose_window_updates_come_after_the_timeout_is_reset() -> Result<()> {
    let (received, outcome, sent) = read_paced(STALL * 2).await?;
    assert!(outcome.is_err(), "the response ended normally");
    assert!(received.len() < sent.len(), "every byte arrived");
    Ok(())
}

/// A small single-frame unary response probed whole (<= 4 KiB) still resets
/// and frees its data when the client withholds flow-control window.
#[tokio::test]
async fn a_small_unary_response_probed_whole_is_reset_when_window_is_withheld() -> Result<()> {
    const TINY_WINDOW: u32 = 512;
    const SMALL_UNARY_PAYLOAD: usize = 2 * 1024;
    let mut server = serve(Bounds::default())?;
    let channel = tonic_client(server.address, TINY_WINDOW).await?;
    let mut stream = open_stream(&channel).await?;
    let mut script = server.next_script().await?;
    script.send_message(SMALL_UNARY_PAYLOAD).await?;
    script.finish().await?;

    wait_until("the stalled small unary response's data is freed", || {
        server.all_freed(1)
    })
    .await?;
    let (_, status) = read_to_end(&mut stream).await?;
    assert_eq!(status, Code::Cancelled);
    Ok(())
}
