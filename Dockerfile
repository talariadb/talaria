FROM alpine:latest AS builder
LABEL maintainer="roman.atachiants@grab.com"

RUN apk --no-cache add ca-certificates
WORKDIR /grab/bin/talaria

# copy the binary
ARG GO_BINARY
COPY "$GO_BINARY" .

# Expose the port and start the service
EXPOSE 8027
CMD ["./talaria"]
