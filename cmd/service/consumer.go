package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
)

const kafkaTopic = "orders"

func (a *App) startKafkaConsumer(ctx context.Context) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	var consumer sarama.Consumer
	var err error

	for i := 0; i < 10; i++ {
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

	pc, err := consumer.ConsumePartition(kafkaTopic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatal("Error consuming partition:", err)
	}
	defer pc.Close()

	log.Println("Connected to Kafka! Listening for messages...")

	for {
		select {
		case <-ctx.Done():
			log.Println("Kafka consumer shutting down...")
			return
		case msg := <-pc.Messages():
			if msg != nil {
				a.processKafkaMessage(msg.Value)
			}
		case err := <-pc.Errors():
			if err != nil {
				log.Println("Kafka error:", err)
			}
		}
	}
}

type Order struct {
	OrderUID        string    `json:"order_uid"`
	TrackNumber     string    `json:"track_number"`
	Entry           string    `json:"entry"`
	Delivery        Delivery  `json:"delivery"`
	Payment         Payment   `json:"payment"`
	Items           []Item    `json:"items"`
	Locale          string    `json:"locale"`
	InternalSig     string    `json:"internal_signature"`
	CustomerID      string    `json:"customer_id"`
	DeliveryService string    `json:"delivery_service"`
	ShardKey        int16     `json:"shardkey,string"`
	SmID            int       `json:"sm_id"`
	DateCreated     time.Time `json:"date_created"`
	OofShard        int16     `json:"oof_shard,string"`
}

type Delivery struct {
	Name    string `json:"name"`
	Phone   string `json:"phone"`
	Zip     string `json:"zip"`
	City    string `json:"city"`
	Address string `json:"address"`
	Region  string `json:"region"`
	Email   string `json:"email"`
}

type Payment struct {
	Transaction  string `json:"transaction"`
	RequestID    string `json:"request_id"`
	Currency     string `json:"currency"`
	Provider     string `json:"provider"`
	Amount       int    `json:"amount"`
	PaymentDT    int64  `json:"payment_dt"`
	Bank         string `json:"bank"`
	DeliveryCost int    `json:"delivery_cost"`
	GoodsTotal   int    `json:"goods_total"`
	CustomFee    int    `json:"custom_fee"`
}

type Item struct {
	ChrtID      int64  `json:"chrt_id"`
	TrackNumber string `json:"track_number"`
	Price       int    `json:"price"`
	Rid         string `json:"rid"`
	Name        string `json:"name"`
	Sale        int    `json:"sale"`
	Size        string `json:"size"`
	TotalPrice  int    `json:"total_price"`
	NmID        int64  `json:"nm_id"`
	Brand       string `json:"brand"`
	Status      int    `json:"status"`
}

func (a *App) processKafkaMessage(msg []byte) {
	log.Printf("Received message: %s", msg)

	var order Order
	if err := json.Unmarshal(msg, &order); err != nil {
		log.Println("Invalid order JSON:", err)
		return
	}

	orderID, err := uuid.Parse(order.OrderUID)
	if err != nil {
		log.Println("Invalid order_uid:", err)
		return
	}
	txUUID, err := uuid.Parse(order.Payment.Transaction)
	if err != nil {
		log.Println("Invalid transaction:", err)
		return
	}

	tx, err := a.DB.Begin()
	if err != nil {
		log.Println("failed to begin tx:", err)
		return
	}
	defer tx.Rollback()
	
	_, err = tx.Exec(`
		INSERT INTO orders (
			order_uid, track_number, entry, locale, internal_signature,
			customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
		)
		ON CONFLICT (order_uid) DO UPDATE
		SET track_number = EXCLUDED.track_number,
			entry = EXCLUDED.entry,
			locale = EXCLUDED.locale,
			internal_signature = EXCLUDED.internal_signature,
			customer_id = EXCLUDED.customer_id,
			delivery_service = EXCLUDED.delivery_service,
			shardkey = EXCLUDED.shardkey,
			sm_id = EXCLUDED.sm_id,
			date_created = EXCLUDED.date_created,
			oof_shard = EXCLUDED.oof_shard`,
		sql.Named("order_uid", orderID),
		sql.Named("track_number", order.TrackNumber),
		sql.Named("entry", order.Entry),
		sql.Named("locale", order.Locale),
		sql.Named("internal_signature", order.InternalSig),
		sql.Named("customer_id", order.CustomerID),
		sql.Named("delivery_service", order.DeliveryService),
		sql.Named("shardkey", order.ShardKey),
		sql.Named("sm_id", order.SmID),
		sql.Named("date_created", order.DateCreated),
		sql.Named("oof_shard", order.OofShard),
	)
	if err != nil {
		log.Println("failed to insert order:", err)
		return
	}

	_, err = tx.Exec(`DELETE FROM deliveries WHERE order_uid=$1`,
		sql.Named("order_uid", orderID))
	if err != nil {
		log.Println("failed to delete old delivery:", err)
		return
	}
	_, err = tx.Exec(`
		INSERT INTO deliveries (id, order_uid, name, phone, zip, city, address, region, email)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		sql.Named("id", uuid.New()),
		sql.Named("order_uid", orderID),
		sql.Named("name", order.Delivery.Name),
		sql.Named("phone", order.Delivery.Phone),
		sql.Named("zip", order.Delivery.Zip),
		sql.Named("city", order.Delivery.City),
		sql.Named("address", order.Delivery.Address),
		sql.Named("region", order.Delivery.Region),
		sql.Named("email", order.Delivery.Email),
	)
	if err != nil {
		log.Println("failed to insert delivery:", err)
		return
	}

	_, err = tx.Exec(`DELETE FROM payments WHERE order_uid=$1`,
		sql.Named("order_uid", orderID))
	if err != nil {
		log.Println("failed to delete old payment:", err)
		return
	}
	reqID := sql.NullString{String: order.Payment.RequestID, Valid: order.Payment.RequestID != ""}
	_, err = tx.Exec(`
		INSERT INTO payments (
			id, order_uid, transaction, request_id, currency, provider,
			amount, payment_dt, bank, delivery_cost, goods_total, custom_fee
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
		)`,
		sql.Named("id", uuid.New()),
		sql.Named("order_uid", orderID),
		sql.Named("transaction", txUUID),
		sql.Named("request_id", reqID),
		sql.Named("currency", order.Payment.Currency),
		sql.Named("provider", order.Payment.Provider),
		sql.Named("amount", order.Payment.Amount),
		sql.Named("payment_dt", order.Payment.PaymentDT),
		sql.Named("bank", order.Payment.Bank),
		sql.Named("delivery_cost", order.Payment.DeliveryCost),
		sql.Named("goods_total", order.Payment.GoodsTotal),
		sql.Named("custom_fee", order.Payment.CustomFee),
	)
	if err != nil {
		log.Println("failed to insert payment:", err)
		return
	}

	_, err = tx.Exec(`DELETE FROM items WHERE order_uid=$1`,
		sql.Named("order_uid", orderID))
	if err != nil {
		log.Println("failed to delete old items:", err)
		return
	}
	for _, it := range order.Items {
		ridUUID, err := uuid.Parse(it.Rid)
		if err != nil {
			log.Println("invalid item.rid:", err)
			return
		}
		_, err = tx.Exec(`
			INSERT INTO items (
				id, order_uid, chrt_id, track_number, price, rid,
				name, sale, size, total_price, nm_id, brand, status
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
			)`,
			sql.Named("id", uuid.New()),
			sql.Named("order_uid", orderID),
			sql.Named("chrt_id", it.ChrtID),
			sql.Named("track_number", it.TrackNumber),
			sql.Named("price", it.Price),
			sql.Named("rid", ridUUID),
			sql.Named("name", it.Name),
			sql.Named("sale", it.Sale),
			sql.Named("size", it.Size),
			sql.Named("total_price", it.TotalPrice),
			sql.Named("nm_id", it.NmID),
			sql.Named("brand", it.Brand),
			sql.Named("status", it.Status),
		)
		if err != nil {
			log.Println("failed to insert item:", err)
			return
		}
	}

	if err = tx.Commit(); err != nil {
		log.Println("failed to commit tx:", err)
		return
	}

	// обновляем кэш
	a.Cache.Put(orderID.String(), msg)
	log.Printf("Order %s saved to DB and cache", orderID)
}
