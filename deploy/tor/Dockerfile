FROM debian:trixie-slim@sha256:8aa5a7bf5a1955bddbe7d74fcdd0b91ab30e74362a8b73af672b0481b35b44a0

RUN apt-get update \
    && apt-get install -y --no-install-recommends tor \
    && rm -rf /var/lib/apt/lists/* \
    && install -d -o debian-tor -g debian-tor -m 0700 /var/lib/tor \
    && chmod g-s /var/lib/tor

USER debian-tor
CMD ["tor", "-f", "/etc/tor/torrc"]
