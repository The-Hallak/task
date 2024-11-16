package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"task/internal"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

var (
	conn     *amqp091.Connection
	client   internal.RabbitClient
	err      error
	products []int
)

func main() {
  log.SetPrefix("INVENTORY SERVICE:")
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

	//creating mock products where the product_id is the index in the array and the quantitiy is the value.
	products = make([]int, 10)
	for i := 0; i < 10; i++ {
		products[i] = rand.Intn(10)
	}

	//consuming order created event
	msgBus, err := client.Consume("OrderCreated", "inventory-service")
	if err != nil {
		panic(err)
	}

	//processing events in new go routine
	go processMessages(msgBus)

	log.Println("Waiting for orders.\nTo stop the program press CTRL+C")

	//blocking main until the user stops the program
	var blocking chan struct{}
	<-blocking
}

func processMessages(msgBus <-chan amqp091.Delivery) {
	for msg := range msgBus {
		//decoding json
		order := internal.Order{}
		err := json.Unmarshal(msg.Body, &order)
		if err != nil {
			// requeue if an error occurred for one time
			if !msg.Redelivered {
				msg.Nack(false, true)
			} else {
				//don't requeue the message again
				msg.Nack(false, false)
				log.Println("error occure while unmarshaling json, msg will be dropped", err)
			}
			continue
		}

		log.Println("got message from order create event:", order)

		//processing order
		if err = processOrder(order); err != nil {
			// requeue if an error occurred
			if !msg.Redelivered {
				msg.Nack(false, true)
			} else {
				//don't requeue the message again
				msg.Nack(false, false)
				log.Println("error occure while processing the order, msg will be dropped", err)
			}
			continue
		}
		// acknowledge the message
		msg.Ack(false)
	}
}

func processOrder(order internal.Order) error {
	//encoding order id to json
	body, err := json.Marshal(order.OrderID)
	if err != nil {
		log.Println("error while marshaling orderId", err)
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	//Out of stock
	outOfStock := order.ProductID >= 10 || products[order.ProductID] < order.Quantity
	if outOfStock {
    //send out of stock event
		client.Send(ctx, "order", "OutOfStock", amqp091.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp091.Persistent,
			Body:         body,
		})
		log.Println("send event out of stock with message:", string(body))
		return nil
	}

	//in stock
	products[order.ProductID] -= order.Quantity
  //send inventory updated event
	client.Send(ctx, "order", "InventoryUpdated", amqp091.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp091.Persistent,
		Body:         body,
	})

	log.Println("send event inventory update with message:", string(body))

	return nil
}
