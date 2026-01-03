FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o /main .

FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata && \
    adduser -D -s /bin/sh appuser

WORKDIR /app

COPY --from=builder /main .
COPY --from=builder /app/config.json ./config.json

RUN chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

CMD ["./main"]