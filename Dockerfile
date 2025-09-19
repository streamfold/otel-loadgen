FROM golang:1.25 AS builder

WORKDIR /app

COPY . /app
RUN make build

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /app/dist/otel-loadgen .

ENTRYPOINT ["./otel-loadgen"]
