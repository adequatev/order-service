package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/golang-migrate/migrate"
	"github.com/golang-migrate/migrate/database/postgres"
	_ "github.com/golang-migrate/migrate/source/file"
	_ "github.com/lib/pq"
)

type App struct {
	DB       *sql.DB
	Cache    *LRUCache
	KafkaBrokers []string
}

func (a *App) getOrderHandler(w http.ResponseWriter, r *http.Request) {
	orderID := r.PathValue("id")
	if orderID == "" {
		http.Error(w, "missing order id", http.StatusBadRequest)
		return
	}

	// check cache
	if val, ok := a.Cache.Get(orderID); ok {
		w.Header().Set("Content-Type", "application/json")
		w.Write(val)
		return
	}

	// one-shot DB query
	var raw json.RawMessage
	err := a.DB.QueryRow(`SELECT 
        json_build_object(
            'order_uid',			o.order_uid,
            'track_number',			o.track_number,
            'entry',				o.entry,
            'locale',				o.locale,
            'internal_signature',	o.internal_signature,
            'customer_id',			o.customer_id,
            'delivery_service',		o.delivery_service,
            'shardkey',				o.shardkey,
            'sm_id',				o.sm_id,
            'date_created',			o.date_created,
            'oof_shard',			o.oof_shard,
            'delivery',				d,
            'payment',				p,
            'items',				(SELECT json_agg(i) FROM items i WHERE i.order_uid=o.order_uid)
        )
        FROM orders o
        LEFT JOIN deliveries d ON d.order_uid=o.order_uid
        LEFT JOIN payments p ON p.order_uid=o.order_uid
        WHERE o.order_uid=$1`, orderID).Scan(&raw)
	if err == sql.ErrNoRows {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		log.Println("getOrderHandler error:", err)
		return
	}

	// update cache
	a.Cache.Put(orderID, raw)

	w.Header().Set("Content-Type", "application/json")
	w.Write(raw)
}

func (a *App) warmupCache() error {
	rows, err := a.DB.Query(`SELECT o.order_uid, 
        json_build_object(
            'order_uid', o.order_uid,
            'track_number', o.track_number,
            'entry', o.entry,
            'locale', o.locale,
            'internal_signature', o.internal_signature,
            'customer_id', o.customer_id,
            'delivery_service', o.delivery_service,
            'shardkey', o.shardkey,
            'sm_id', o.sm_id,
            'date_created', o.date_created,
            'oof_shard', o.oof_shard,
            'delivery', d,
            'payment', p,
            'items', (SELECT json_agg(i) FROM items i WHERE i.order_uid=o.order_uid)
        ) AS data
        FROM orders o
        LEFT JOIN deliveries d ON d.order_uid=o.order_uid
        LEFT JOIN payments p ON p.order_uid=o.order_uid
		LIMIT $1`, a.Cache.capacity)
	if err != nil {
		return fmt.Errorf("warmup query failed: %w", err)
	}
	defer rows.Close()
	
	count := 0
	for rows.Next() {
		var orderUID string
		var raw json.RawMessage
		if err := rows.Scan(&orderUID, &raw); err != nil {
			return err
		}
		a.Cache.Put(orderUID, raw)
		count++
	}
	log.Printf("Cache warmup complete: %d orders loaded", count)
	return nil
}

func main() {
	// подключение к Postgres
	connStr := "postgres://someuser:some_password@localhost:5432/orders?sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Database connection error:", err)
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		log.Fatal("Database ping error:", err)
	}
	
	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		log.Fatal("Fail creating migrate driver", err)
	}
	migration, err := migrate.NewWithDatabaseInstance("file://./migrations", "postgres", driver);
	if err != nil {
		log.Fatal("Fail migrating", err)
	}
	if err = migration.Up(); err != nil && err != migrate.ErrNoChange {
		log.Fatal("Fail applying", err)
	}
	
	app := &App{
		DB:          db,
		Cache:       NewLRUCache(3),
		KafkaBrokers: []string{"localhost:9092"},
	}
	
	if err := app.warmupCache(); err != nil {
		log.Printf("Cache warmup failed: %v", err)
	}
	
	// HTTP сервер
	mux := http.NewServeMux()
	mux.HandleFunc("GET /order/{id}", app.getOrderHandler)
	mux.Handle("/", http.FileServer(http.Dir("./static")))
	handler := loggingMiddleware(mux)
	srv := &http.Server{
		Addr:    ":8080",
		Handler: handler,
	}

	// graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// запускаем Kafka consumer
	go app.startKafkaConsumer(ctx)

	// запускаем HTTP сервер
	go func() {
		log.Println("Server starting on :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("ListenAndServe error: %v", err)
		}
	}()

	<-ctx.Done()
	stop()
	log.Println("Shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Server Shutdown Failed:%+v", err)
	}
}
