FROM ubuntu:22.04

ARG TARGETARCH

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    tzdata && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY otela-${TARGETARCH} otela
RUN chmod +x otela

ENTRYPOINT ["/app/otela"]
