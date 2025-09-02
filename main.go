package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	_ "github.com/lib/pq"
)

var db *sql.DB;
func main() {
	connStr := "postgres://someuser:some_password@localhost:5432/orders?sslmode=disable"
	var err error;
	db, err = sql.Open("postgres", connStr)
	if err != nil{
		panic("could not connect to database")
	}
	defer db.Close()
	
	if err = db.Ping(); err != nil {
		log.Fatal("Database ping error:", err)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS orders (
			order_uid VARCHAR(255) PRIMARY KEY,
			data JSONB NOT NULL
		)`)
	if err != nil {
		log.Fatal("Create table error:", err)
	}

	http.HandleFunc("/order/", getOrderHandler)
	http.HandleFunc("/save", saveOrderHandler)
	http.Handle("/", http.FileServer(http.Dir(".")))
	
	log.Println("Server starting on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func getOrderHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", 405)
		return
	}

	orderUID := r.URL.Path[len("/order/"):]
	if orderUID == "" {
		http.Error(w, "Order UID required", 400)
		return
	}

	var data []byte
	err := db.QueryRow("SELECT data FROM orders WHERE order_uid = $1", orderUID).Scan(&data)
	if err != nil {
		if err == sql.ErrNoRows {
			http.Error(w, "Order not found", 404)
		} else {
			http.Error(w, "Database error", 500)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func saveOrderHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", 405)
		return
	}

	var request struct {
		OrderUID string          `json:"order_uid"`
		Data     json.RawMessage `json:"data"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid JSON", 400)
		return
	}
	
	if request.OrderUID == "" {
		http.Error(w, "Order UID required", 400)
		return
	}

	_, err := db.Exec(`
		INSERT INTO orders (order_uid, data) 
		VALUES ($1, $2) 
		ON CONFLICT (order_uid) 
		DO UPDATE SET data = EXCLUDED.data`,
		request.OrderUID, request.Data)

	if err != nil {
		http.Error(w, "Database error", 500)
		return
	}

	w.WriteHeader(201)
	fmt.Fprintf(w, "Order %s saved", request.OrderUID)
}