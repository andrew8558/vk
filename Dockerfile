FROM golang:1.24.2

WORKDIR /app
COPY . .

RUN go build -o /build ./cmd \
    && go clean -cache -modcache

EXPOSE 8080

ENTRYPOINT ["/build"]