package internal

import (
	"context"
	"fmt"

	"github.com/rabbitmq/amqp091-go"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Order struct {
	OrderID   int `json:"order_id" validate:"required,gte=0"`
	ProductID int `json:"product_id" validate:"required,gte=0"`
	Quantity  int `json:"quantity" validate:"required,gte=1"`
}

type RabbitClient struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

func ConnectRabbitMQ(username, password, host string) (*amqp.Connection, error) {
	return amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s", username, password, host))
}

func NewRabbitMQClient(conn *amqp.Connection) (RabbitClient, error) {
	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	if err := ch.ExchangeDeclare("order", "direct", true, false, false, false, nil); err != nil {
		panic(err)
	}

	declaringQueues(ch)

	bindingQueues(ch)

	return RabbitClient{
		conn: conn,
		ch:   ch,
	}, nil
}

func declaringQueues(ch *amqp091.Channel) {
	if _, err := ch.QueueDeclare("OrderCreated", true, false, false, false, nil); err != nil {
		panic(err)
	}
	if _, err := ch.QueueDeclare("InventoryUpdated", true, false, false, false, nil); err != nil {
		panic(err)
	}
	if _, err := ch.QueueDeclare("OutOfStock", true, false, false, false, nil); err != nil {
		panic(err)
	}
}

func bindingQueues(ch *amqp091.Channel) {
	if err := ch.QueueBind("OrderCreated", "OrderCreated", "order", false, nil); err != nil {
		panic(err)
	}
	if err := ch.QueueBind("InventoryUpdated", "InventoryUpdated", "order", false, nil); err != nil {
		panic(err)
	}
	if err := ch.QueueBind("OutOfStock", "OutOfStock", "order", false, nil); err != nil {
		panic(err)
	}
}

func (rc RabbitClient) Close() error {
	return rc.ch.Close()
}

func (rc RabbitClient) Send(ctx context.Context, exchange, routingkey string, options amqp.Publishing) error {
	return rc.ch.PublishWithContext(ctx, exchange, routingkey, true, false, options)
}

func (rc RabbitClient) Consume(queue, consumer string) (<-chan amqp.Delivery, error) {
	return rc.ch.Consume(queue, consumer, false, false, false, false, nil)
}
