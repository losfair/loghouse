# syntax=docker/dockerfile:1

FROM golang:1.25-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/loghouse ./cmd/loghouse

FROM alpine:3.20
RUN apk add --no-cache ca-certificates \
    && addgroup -S loghouse \
    && adduser -S -G loghouse loghouse \
    && mkdir -p /run/loghouse \
    && chown -R loghouse:loghouse /run/loghouse

COPY --from=build /out/loghouse /usr/local/bin/loghouse

USER loghouse
ENV LOGHOUSE_PIPE_PATH=/run/loghouse/loghouse.pipe \
    LOGHOUSE_METRICS_ADDR=0.0.0.0:2112

EXPOSE 2112
ENTRYPOINT ["/usr/local/bin/loghouse"]
