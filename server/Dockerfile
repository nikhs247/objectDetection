FROM nikhs247/gocv:latest

WORKDIR /app
ADD . /app/
RUN go mod download
RUN go build -o main server.go
ENTRYPOINT ["./main localhost 8080"]