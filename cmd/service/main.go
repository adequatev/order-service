package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"
)

const kafkaTopic = "orders"

type App struct {
	DB       *sql.DB
	Cache    *LRUCache
	KafkaBrokers []string
}

func (a *App) getOrderHandler(w http.ResponseWriter, r *http.Request) {
	orderUID := r.PathValue("id")
	if orderUID == "" {
		http.Error(w, "Order UID required", http.StatusBadRequest)
		return
	}

	// сначала пробуем из кэша
	if data, ok := a.Cache.Get(orderUID); ok {
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
		return
	}

	// иначе — из БД
	var data []byte
	err := a.DB.QueryRow("SELECT data FROM orders WHERE order_uid = $1", orderUID).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		http.Error(w, "Order not found", http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		log.Println("DB error:", err)
		return
	}

	// кладем в кэш
	a.Cache.Put(orderUID, data)
	saveCacheToDb(a, orderUID)
	
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func saveCacheToDb(a *App, orderUID string) {
    _, err := a.DB.Exec(`
        INSERT INTO cache_orders (order_uid)
        VALUES ($1)
        ON CONFLICT (order_uid) DO NOTHING`, orderUID)
    if err != nil {
        log.Println("Error saving cache UID:", err)
    }
}

func warmUpCache(a *App) {
	db := a.DB;
	cache := a.Cache;
    rows, err := db.Query("SELECT order_uid FROM cache_orders")
    if err != nil {
        log.Println("Error loading cache keys:", err)
        return
    }
    defer rows.Close()

    for rows.Next() {
        var uid string
        if err := rows.Scan(&uid); err != nil {
            log.Println("Error scanning uid:", err)
            continue
        }

        var data []byte
        err := db.QueryRow("SELECT data FROM orders WHERE order_uid = $1", uid).Scan(&data)
        if err != nil {
            log.Println("Error loading order from DB:", err)
            continue
        }

        cache.Put(uid, data)
    }

    log.Println("Cache warm-up completed")
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

	// создаем таблицы
	_, err = db.Exec(`
CREATE TABLE IF NOT EXISTS orders (
	order_uid VARCHAR(255) PRIMARY KEY,
	data JSONB NOT NULL
);
CREATE TABLE IF NOT EXISTS cache_orders (
	order_uid VARCHAR(255) PRIMARY KEY
);
`)
	if err != nil {
		log.Fatal("Create table error:", err)
	}
	
	app := &App{
		DB:          db,
		Cache:       NewLRUCache(3),
		KafkaBrokers: []string{"localhost:9092"},
	}
	
	warmUpCache(app);
	
	// HTTP сервер
	mux := http.NewServeMux()
	mux.HandleFunc("GET /order/{id}", app.getOrderHandler)
	mux.Handle("/", http.FileServer(http.Dir(".")))
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
