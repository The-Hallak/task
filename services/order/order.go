package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"task/internal"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/rabbitmq/amqp091-go"
)

var (
	conn   *amqp091.Connection
	client internal.RabbitClient
	err    error
)

func main() {
  log.SetPrefix("ORDER SERVICE:")
	conn, err = internal.ConnectRabbitMQ("guest", "guest", "localhost")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	client, err = internal.NewRabbitMQClient(conn)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	//receiving orders via http request
	http.HandleFunc("/order", handleOrder)
	log.Println("Server started on port 8080. Waiting for orders.\nTo close the program press CTRL+C")
	if err = http.ListenAndServe(":8080", nil); err != nil {
		panic(err)
	}
}

func handleOrder(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	//decoding json
	order := internal.Order{}
	if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
		log.Println("error while decoding request", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	//validation
	validate := validator.New(validator.WithRequiredStructEnabled())
	if err := validate.Struct(order); err != nil {
		log.Println("bad order", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.Write([]byte("order received successfully"))

	//encoding the order to json
	msg, err := json.Marshal(order)
	if err != nil {
		log.Println(err)
		return
	}

	//send the orderCreated event
	client.Send(ctx, "order", "OrderCreated", amqp091.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp091.Persistent,
		Body:         msg,
	})
	log.Println("send event order created with message:", string(msg))

}
