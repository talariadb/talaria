FROM golang:1.17 AS builder

ARG GO111MODULE="on"
ARG GOOS="linux"
ARG GOARCH="amd64"
ENV GO111MODULE=${GO111MODULE}
ENV GOOS=${GOOS}
ENV GOARCH=${GOARCH}

# GOPATH => /go
RUN mkdir -p /go/src/talaria
COPY . src/talaria
RUN cd src/talaria && go build . && test -x talaria

FROM debian:latest AS base
ARG MAINTAINER=roman.atachiants@gmail.com
LABEL maintainer=${MAINTAINER}

# # add ca certificates for http secured connection
RUN apt-get update && apt-get install -y ca-certificates && apt-get install -y dumb-init && rm -rf /var/cache/apk/*

WORKDIR /root/  
ARG GO_BINARY=talaria
COPY  --from=builder /go/src/talaria/${GO_BINARY} .
#COPY  ${GO_BINARY} .

ADD entrypoint.sh . 
RUN mkdir /etc/talaria/ && chmod +x /root/${GO_BINARY} /root/entrypoint.sh
ENV TALARIA_RC=/etc/talaria/talaria.rc 
# # Expose the port and start the service
EXPOSE 8027
ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["bash", "-c", "/root/entrypoint.sh"]
