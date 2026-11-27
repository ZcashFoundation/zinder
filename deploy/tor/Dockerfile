FROM debian:trixie-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends tor \
    && rm -rf /var/lib/apt/lists/* \
    && install -d -o debian-tor -g debian-tor -m 0700 /var/lib/tor \
    && chmod g-s /var/lib/tor

USER debian-tor
CMD ["tor", "-f", "/etc/tor/torrc"]
