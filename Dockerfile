FROM alpine:latest AS builder
LABEL maintainer="roman.atachiants@grab.com"

# add modules
RUN apk --no-cache add ca-certificates gcompat libc6-compat
WORKDIR /root/  

# copy the binary
ARG GO_BINARY
COPY "$GO_BINARY" .
RUN chmod +x /root/talaria

# Expose the port and start the service
EXPOSE 8027
CMD ["/root/talaria"]
