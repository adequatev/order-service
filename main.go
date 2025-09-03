package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	_ "github.com/lib/pq"
)

var db *sql.DB;
func main() {
	// Подключение к PostgreSQL
	connStr := "postgres://someuser:some_password@localhost:5432/orders?sslmode=disable"
	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Database connection error:", err)
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		log.Fatal("Database ping error:", err)
	}

	// Создаем таблицу
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS orders (
			order_uid VARCHAR(255) PRIMARY KEY,
			data JSONB NOT NULL
		)`)
	if err != nil {
		log.Fatal("Create table error:", err)
	}

	// Запускаем Kafka consumer
	go startKafkaConsumer()

	// HTTP сервер
	http.HandleFunc("/order/", getOrderHandler)
	http.Handle("/", http.FileServer(http.Dir(".")))

	log.Println("Server starting on :8080")
	go func() {
		log.Fatal(http.ListenAndServe(":8080", nil))
	}()

	// Ожидаем сигнал для graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	<-sigchan
	log.Println("Shutting down...")
}

func startKafkaConsumer() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	_attempts := 10;
	
	// Пытаемся подключиться к Kafka с таймаутом
	for i := range _attempts {
		consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
		if err != nil {
			log.Printf("Attempt %d: Kafka not available, retrying...", i+1)
			time.Sleep(5 * time.Second)
			continue
		}
		defer consumer.Close()

		partitionConsumer, err := consumer.ConsumePartition("orders", 0, sarama.OffsetNewest)
		if err != nil {
			log.Fatal("Error consuming partition:", err)
		}
		defer partitionConsumer.Close()

		log.Println("Connected to Kafka! Listening for messages...")

		// Обрабатываем сообщения
		for {
			select {
			case msg := <-partitionConsumer.Messages():
				processKafkaMessage(msg.Value)
			case err := <-partitionConsumer.Errors():
				log.Println("Kafka error:", err)
			}
		}
	}

	log.Fatalf("Failed to connect to Kafka after %d attempts", _attempts)
}

func processKafkaMessage(message []byte) {
	log.Printf("Received message: %s", string(message))

	// Простая валидация JSON
	if !json.Valid(message) {
		log.Println("Invalid JSON")
		return
	}

	// Парсим чтобы извлечь order_uid
	var order struct {
		OrderUID string `json:"order_uid"`
	}
	if err := json.Unmarshal(message, &order); err != nil {
		log.Println("Error parsing order_uid:", err)
		return
	}

	if order.OrderUID == ""{
		log.Println("Missing order_uid")
		return
	}

	// Сохраняем в базу
	_, err := db.Exec(`
		INSERT INTO orders (order_uid, data) 
		VALUES ($1, $2) 
		ON CONFLICT (order_uid) 
		DO UPDATE SET data = EXCLUDED.data`,
		order.OrderUID, message)

	if err != nil {
		log.Println("Error saving to database:", err)
		return
	}

	log.Printf("Order %s saved to DB", order.OrderUID)
}

func getOrderHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET"{
		http.Error(w, "Method not allowed", 405)
		return
	}

	orderUID := r.URL.Path[len("/order/"):]
	if orderUID == ""{
		http.Error(w, "Order UID required", 400)
		return
	}

	var data []byte
	err := db.QueryRow("SELECT data FROM orders WHERE order_uid = $1", orderUID).Scan(&data)
	if err != nil {
		http.Error(w, "Order not found", 404)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}