package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/brianvoe/gofakeit/v7"
)

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
	Transaction   string `json:"transaction"`
	RequestID     string `json:"request_id"`
	Currency      string `json:"currency"`
	Provider      string `json:"provider"`
	Amount        int    `json:"amount"`
	PaymentDT     int64  `json:"payment_dt"`
	Bank          string `json:"bank"`
	DeliveryCost  int    `json:"delivery_cost"`
	GoodsTotal    int    `json:"goods_total"`
	CustomFee     int    `json:"custom_fee"`
}

type Item struct {
	ChrtID      int    `json:"chrt_id"`
	TrackNumber string `json:"track_number"`
	Price       int    `json:"price"`
	RID         string `json:"rid"`
	Name        string `json:"name"`
	Sale        int    `json:"sale"`
	Size        string `json:"size"`
	TotalPrice  int    `json:"total_price"`
	NmID        int    `json:"nm_id"`
	Brand       string `json:"brand"`
	Status      int    `json:"status"`
}

type Order struct {
	OrderUID         string    `json:"order_uid"`
	TrackNumber      string    `json:"track_number"`
	Entry            string    `json:"entry"`
	Delivery         Delivery  `json:"delivery"`
	Payment          Payment   `json:"payment"`
	Items            []Item    `json:"items"`
	Locale           string    `json:"locale"`
	InternalSign     string    `json:"internal_signature"`
	CustomerID       string    `json:"customer_id"`
	DeliveryService  string    `json:"delivery_service"`
	ShardKey         string    `json:"shardkey"`
	SmID             int       `json:"sm_id"`
	DateCreated      time.Time `json:"date_created"`
	OofShard         string    `json:"oof_shard"`
}

func main() {
	// конфиг Kafka producer
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	log.Println("Publisher started. Sending fake orders every 2s...")

	for {
		order := generateFakeOrder()

		data, err := json.Marshal(order)
		if err != nil {
			log.Println("Error marshaling order:", err)
			continue
		}

		msg := &sarama.ProducerMessage{
			Topic: "orders",
			Value: sarama.ByteEncoder(data),
		}

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Println("Error publishing message:", err)
		} else {
			log.Printf("Published order %s to partition %d at offset %d",
				order.OrderUID, partition, offset)
		}

		time.Sleep(2 * time.Second)
	}
}

func generateFakeOrder() Order {
	uid := gofakeit.UUID()
	track := gofakeit.LetterN(4) + gofakeit.DigitN(8)

	amount := gofakeit.Number(500, 5000)
	deliveryCost := gofakeit.Number(200, 1000)
	goodsTotal := amount - deliveryCost

	return Order{
		OrderUID:    uid,
		TrackNumber: track,
		Entry:       gofakeit.RandomString([]string{"WBIL", "MKT", "SHOP"}),
		Delivery: Delivery{
			Name:    gofakeit.Name(),
			Phone:   gofakeit.Phone(),
			Zip:     gofakeit.Zip(),
			City:    gofakeit.City(),
			Address: gofakeit.Street() + gofakeit.StreetNumber(),
			Region:  gofakeit.State(),
			Email:   gofakeit.Email(),
		},
		Payment: Payment{
			Transaction:  uid,
			RequestID:    "",
			Currency:     "USD",
			Provider:     gofakeit.RandomString([]string{"wbpay", "paypal", "stripe"}),
			Amount:       amount,
			PaymentDT:    time.Now().Unix(),
			Bank:         gofakeit.Company(),
			DeliveryCost: deliveryCost,
			GoodsTotal:   goodsTotal,
			CustomFee:    0,
		},
		Items: []Item{
			{
				ChrtID:      gofakeit.Number(1000000, 9999999),
				TrackNumber: track,
				Price:       goodsTotal,
				RID:         gofakeit.UUID(),
				Name:        gofakeit.ProductName(),
				Sale:        gofakeit.Number(0, 50),
				Size:        gofakeit.RandomString([]string{"S", "M", "L", "XL"}),
				TotalPrice:  goodsTotal,
				NmID:        gofakeit.Number(100000, 999999),
				Brand:       gofakeit.Company(),
				Status:      gofakeit.Number(100, 300),
			},
		},
		Locale:          "en",
		InternalSign:    "",
		CustomerID:      gofakeit.Username(),
		DeliveryService: gofakeit.RandomString([]string{"meest", "dhl", "ups"}),
		ShardKey:        gofakeit.DigitN(1),
		SmID:            gofakeit.Number(10, 999),
		DateCreated:     time.Now().UTC(),
		OofShard:        gofakeit.DigitN(1),
	}
}
