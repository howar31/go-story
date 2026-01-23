FROM golang:1.23 AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /app/server .

FROM gcr.io/distroless/base-debian12

WORKDIR /app
COPY --from=builder /app/server /app/server

ENV PORT=8080
EXPOSE 8080

# 必須在部署時提供：
# - DATABASE_URL：Postgres 連線字串
# - STATICS_HOST：靜態圖片 host，例如 https://v3-statics-dev.mirrormedia.mg/images
ENTRYPOINT ["/app/server"]

