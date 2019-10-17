FROM ubuntu:18.04

# GO_BINARY is passed to this dockerfile by Joel's CIv2 sauce
ARG GO_BINARY

RUN mkdir -p /grab/bin \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -y --force-yes ca-certificates
COPY "$GO_BINARY" /grab/bin/talaria
ENTRYPOINT /grab/bin/talaria
EXPOSE 8027
