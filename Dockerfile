# FROM debian:latest AS builder
# LABEL maintainer="roman.atachiants@gmail.com"

# # add ca certificates for http secured connection
# RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/cache/apk/*

# # copy the binary
# WORKDIR /root/  
# ARG GO_BINARY
# COPY "$GO_BINARY" .
# RUN chmod +x /root/talaria

# # Expose the port and start the service
# EXPOSE 8027
# CMD ["/root/talaria"]

FROM golang:1.14 AS builder
LABEL maintainer="roman.atachiants@gmail.com"

# Copy the directory into the container outside of the gopath
RUN mkdir -p /go/src/github.com/kelindar/talaria/
WORKDIR /go/src/github.com/kelindar/talaria/
ADD . /go/src/github.com/kelindar/talaria/

# Download and install any required third party dependencies into the container.
RUN go build -o /go/bin/talaria .

# Base image for runtime
FROM debian:latest
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/cache/apk/*

WORKDIR /root/
COPY --from=builder /go/bin/talaria .
RUN chmod +x ./talaria

EXPOSE 8027
CMD ["./talaria"]