package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func (a *App) startKafkaConsumer(ctx context.Context) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	var consumer sarama.Consumer
	var err error
	attempts := 10

	// retry подключение
	for i := range attempts {
		consumer, err = sarama.NewConsumer(a.KafkaBrokers, config)
		if err != nil {
			log.Printf("Attempt %d: Kafka not available, retrying...", i+1)
			time.Sleep(5 * time.Second)
			continue
		}
		break
	}
	if err != nil {
		log.Fatalf("Failed to connect to Kafka: %v", err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(kafkaTopic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatal("Error consuming partition:", err)
	}
	defer partitionConsumer.Close()

	log.Println("Connected to Kafka! Listening for messages...")

	for {
		select {
		case <-ctx.Done():
			log.Println("Kafka consumer shutting down...")
			return
		case msg := <-partitionConsumer.Messages():
			a.processKafkaMessage(msg.Value)
		case err := <-partitionConsumer.Errors():
			log.Println("Kafka error:", err)
		}
	}
}

func (a *App) processKafkaMessage(message []byte) {
	log.Printf("Received message: %s", string(message))

	// валидация JSON
	var order struct {
		OrderUID string `json:"order_uid"`
	}
	if err := json.Unmarshal(message, &order); err != nil || order.OrderUID == "" {
		log.Println("Invalid order JSON:", err)
		return
	}

	// запись в БД
	_, err := a.DB.Exec(`
		INSERT INTO orders (order_uid, data) 
		VALUES ($1, $2) 
		ON CONFLICT (order_uid) 
		DO UPDATE SET data = EXCLUDED.data`,
		order.OrderUID, message)
	if err != nil {
		log.Println("Error saving to database:", err)
		return
	}

	// обновляем кэш
	a.Cache.Put(order.OrderUID, message)

	log.Printf("Order %s saved to DB and cache", order.OrderUID)
}
