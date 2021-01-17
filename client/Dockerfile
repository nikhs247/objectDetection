FROM nikhs247/gocv:latest

WORKDIR /app
ADD . /app/
RUN go mod download
RUN go build -o client client.go
ENTRYPOINT ["./client"]