package main

import (
	"encoding/json"
	"log"
	"task/internal"

	"github.com/rabbitmq/amqp091-go"
)

var (
	conn   *amqp091.Connection
	client internal.RabbitClient
	err    error
)

func main() {
	log.SetPrefix("NOOTIFICATION SERVICE:")
	if conn, err = internal.ConnectRabbitMQ("guest", "guest", "localhost"); err != nil {
		panic(err)
	}
	defer conn.Close()

	if client, err = internal.NewRabbitMQClient(conn); err != nil {
		panic(err)
	}
	defer client.Close()

	//consuming out of stock event
	outOfStockMsgBus, err := client.Consume("OutOfStock", "notification-service")
	if err != nil {
		panic(err)
	}

	//consuming inventory updated event
	inventoryUpdatedMsgBus, err := client.Consume("InventoryUpdated", "inventory-service")
	if err != nil {
		panic(err)
	}

	//process events in new go routine
	go processMessages(outOfStockMsgBus, "OutOfStock")
	go processMessages(inventoryUpdatedMsgBus, "InventoryUpdated")

	log.Println("Waiting for orders.\nTo stop the program press CTRL+C")
	//blocking main until the use stops the program
	var blocking chan struct{}
	<-blocking
}

func processMessages(msgBus <-chan amqp091.Delivery, event string) {
	for msg := range msgBus {
		//decode json to get order id
		var orderID int
		if err = json.Unmarshal(msg.Body, &orderID); err != nil {
			if !msg.Redelivered {
				msg.Nack(false, true)
			} else {
				//don't requeue the message again
				msg.Nack(false, false)
				log.Println("error occure while unmarshaling json, msg will be dropped", err)
			}
			continue
		}
		//mock notification
		if event == "OutOfStock" {
			log.Printf("order number %d didn't compleate because it's out of stock", orderID)
		} else {
			log.Printf("order number %d was processed", orderID)
		}

		// acknowledge the message
		msg.Ack(false)
	}
}
