FROM nikhs247/gocv:latest

WORKDIR /app
ADD go.mod go.sum /app/
RUN go clean -modcache
RUN go mod download
RUN go mod tidy
ADD server /app/server
RUN go build -o main server/server.go
ENTRYPOINT ["./main localhost 8080"]