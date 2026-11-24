FROM golang:1.27-alpine AS builder

RUN apk add --no-cache ca-certificates

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/notification ./cmd/api

FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /out/notification /notification

# Usuario sin privilegios, como el resto de los servicios. El archivo de credenciales de Firebase
# que se monta en /app tiene que ser legible por este UID.
USER 65532:65532

EXPOSE 8080

ENTRYPOINT ["/notification"]
