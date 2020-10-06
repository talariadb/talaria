FROM debian:latest AS builder
LABEL maintainer="roman.atachiants@gmail.com"

# add ca certificates for http secured connection
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/cache/apk/*

# copy the binary
WORKDIR /root/  
ARG GO_BINARY
COPY "$GO_BINARY" .
RUN chmod +x /root/talaria

# Expose the port and start the service
EXPOSE 8027
CMD ["/root/talaria"]