FROM golang:latest

WORKDIR /app

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

RUN CGO_ENABLED=1 go build -o /app/server main.go

# TODO: Export to scratch image for deployment. Blocked by linking.

EXPOSE 8000

ENTRYPOINT ["/app/server"]