#!/usr/bin/env python3
"""Loopback TCP access to a fixture's internal Docker network, without an outbound route."""
import asyncio
from contextlib import suppress
import hmac
import json
from pathlib import Path
import secrets
import signal
import sys

MAX_FORWARD_STREAMS = 64
MAX_CONTROL_CONNECTIONS = 4

# Docker Desktop cannot route from the host to an internal bridge. Run the TCP
# stream inside the pinned node image, with Docker carrying stdin and stdout.
EXEC_TCP = r'''
use IO::Socket::INET;
use POSIX qw(_exit);
my $socket = IO::Socket::INET->new(PeerHost => $ARGV[0], PeerPort => $ARGV[1], Timeout => 5)
    or die "connect fixture endpoint: $!";
sub copy_stream {
    my ($from, $to) = @_;
    while (my $size = sysread($from, my $bytes, 65536)) {
        my $offset = 0;
        while ($offset < $size) {
            my $written = syswrite($to, $bytes, $size - $offset, $offset) or return;
            $offset += $written;
        }
    }
}
my $upload = fork();
defined($upload) or die "fork fixture TCP stream: $!";
if (!$upload) {
    copy_stream(\*STDIN, $socket);
    shutdown($socket, 1);
    _exit(0);
}
$SIG{PIPE} = 'IGNORE';
copy_stream($socket, \*STDOUT);
kill 'TERM', $upload;
waitpid($upload, 0);
'''


async def copy_stream(source, destination):
    while chunk := await source.read(65536):
        destination.write(chunk)
        await destination.drain()
    with suppress(OSError):
        destination.write_eof()


async def forward(reader, writer, host, port, exec_container):
    process = None
    upstream_writer = None
    streams = []
    try:
        if exec_container:
            process = await asyncio.create_subprocess_exec(
                'docker', 'exec', '-i', exec_container, 'perl', '-e', EXEC_TCP, host, str(port),
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE)
            upstream_reader, upstream_writer = process.stdout, process.stdin
        else:
            upstream_reader, upstream_writer = await asyncio.open_connection(host, port)
        streams = [
            asyncio.create_task(copy_stream(reader, upstream_writer)),
            asyncio.create_task(copy_stream(upstream_reader, writer)),
        ]
        await asyncio.gather(*streams)
    except OSError:
        pass
    finally:
        for stream in streams:
            stream.cancel()
        await asyncio.gather(*streams, return_exceptions=True)
        if upstream_writer:
            upstream_writer.close()
        if process:
            with suppress(ProcessLookupError):
                process.kill()
            await process.communicate()


async def main():
    # The ownership token in argv lets the operator verify a saved PID before signalling it.
    path, _owner = sys.argv[1:]
    routes = json.loads(Path(path).read_text())
    managed = isinstance(routes, dict)
    targets = routes['targets'] if managed else routes
    exec_container = routes['exec_container'] if managed else None
    forwarding_tasks = set()
    control_tasks = set()
    stopping = asyncio.Event()
    stopped = asyncio.Event()
    servers = []

    def accept_forward(reader, writer, host, port):
        if stopping.is_set() or len(forwarding_tasks) >= MAX_FORWARD_STREAMS:
            writer.close()
            return
        task = asyncio.create_task(forward(reader, writer, host, port, exec_container))
        forwarding_tasks.add(task)

        def close_forward(completed):
            writer.close()
            forwarding_tasks.discard(completed)

        task.add_done_callback(close_forward)

    token = secrets.token_hex(32).encode('ascii')

    async def stop(reader, writer):
        try:
            received = await asyncio.wait_for(reader.readexactly(len(token)), timeout=1)
            if hmac.compare_digest(received, token):
                stopping.set()
                await stopped.wait()
                writer.write(b'stopped\n')
                await writer.drain()
        except (OSError, EOFError, TimeoutError):
            pass
        finally:
            writer.close()
            with suppress(OSError):
                await writer.wait_closed()

    def accept_control(reader, writer):
        if stopping.is_set() or len(control_tasks) >= MAX_CONTROL_CONNECTIONS:
            writer.close()
            return
        task = asyncio.create_task(stop(reader, writer))
        control_tasks.add(task)
        task.add_done_callback(control_tasks.discard)

    try:
        for local, host, port in targets:
            servers.append(await asyncio.start_server(
                lambda r, w, h=host, p=port: accept_forward(r, w, h, p), '127.0.0.1', local))
        if not managed:
            await asyncio.gather(*(server.serve_forever() for server in servers))
            return
        if sys.platform != 'win32':
            asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stopping.set)
        control = await asyncio.start_server(accept_control, '127.0.0.1', 0)
        servers.append(control)
        control_path = Path(path).with_name('relay-control.json')
        pending = control_path.with_suffix('.pending')
        pending.write_text(json.dumps(dict(port=control.sockets[0].getsockname()[1], token=token.decode('ascii'))))
        pending.replace(control_path)
        await stopping.wait()
    finally:
        for server in servers:
            server.close()
        for task in forwarding_tasks:
            task.cancel()
        await asyncio.gather(*forwarding_tasks, return_exceptions=True)
        stopped.set()
        await asyncio.gather(*control_tasks, return_exceptions=True)
        for server in servers:
            await server.wait_closed()


if __name__ == '__main__':
    asyncio.run(main())
