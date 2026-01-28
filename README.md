# RMF-Latency-Measure
Measure the latency of RabbitMQ consumer for RMF using Go

# work on code
go run .

# add lib
go get github.com/rabbitmq/amqp091-go

# add/remove deps
go mod tidy

# format
go fmt ./...

# check what changed
git status