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
	DB   	*sql.DB
	Cache	*LRUCache
	KafkaBrokers []string
}

func (a *App) getOrderHandler(w http.ResponseWriter, r *http.Request) {
	orderID := r.PathValue("id")
	if orderID == "" {
		http.Error(w, "missing order id", http.StatusBadRequest)
		return
	}
	
	if val, ok := a.Cache.Get(orderID); ok {
		w.Header().Set("Content-Type", "application/json")
		w.Write(val)
		return
	}

	query :=
	`
	SELECT 
    o.order_uid,
    o.track_number,
    o.entry,
    o.locale,
    o.internal_signature,
    o.customer_id,
    o.delivery_service,
    o.shardkey,
    o.sm_id,
    o.date_created,
    o.oof_shard,

    d.name          AS delivery_name,
    d.phone         AS delivery_phone,
    d.zip           AS delivery_zip,
    d.city          AS delivery_city,
    d.address       AS delivery_address,
    d.region        AS delivery_region,
    d.email         AS delivery_email,

    p.transaction   AS payment_transaction,
    p.request_id    AS payment_request_id,
    p.currency      AS payment_currency,
    p.provider      AS payment_provider,
    p.amount        AS payment_amount,
    p.payment_dt    AS payment_dt,
    p.bank          AS payment_bank,
    p.delivery_cost AS payment_delivery_cost,
    p.goods_total   AS payment_goods_total,
    p.custom_fee    AS payment_custom_fee,

    i.chrt_id       AS item_chrt_id,
    i.track_number  AS item_track_number,
    i.price         AS item_price,
	i.rid           AS item_rid,
	i.name          AS item_name,
    i.sale          AS item_sale,
    i.size          AS item_size,
    i.total_price   AS item_total_price,
    i.nm_id         AS item_nm_id,
    i.brand         AS item_brand,
    i.status        AS item_status

FROM orders o
LEFT JOIN deliveries d ON d.order_uid = o.order_uid
LEFT JOIN payments p   ON p.order_uid = o.order_uid
LEFT JOIN items i      ON i.order_uid = o.order_uid
WHERE o.order_uid = $1;	
	`;
	
	var order Order;
	var delivery Delivery;
	var payment Payment;
	var items []Item;
	flag := false;
	rows, err := a.DB.Query(query, orderID);
	for rows.Next() {
		var itm Item

		err := rows.Scan(
			&order.OrderUID, &order.TrackNumber, &order.Entry, &order.Locale, &order.InternalSig,
			&order.CustomerID, &order.DeliveryService, &order.ShardKey, &order.SmID,
			&order.DateCreated, &order.OofShard,
			
			&delivery.Name, &delivery.Phone, &delivery.Zip,
			&delivery.City, &delivery.Address, &delivery.Region, &delivery.Email,
			
			&payment.Transaction, &payment.RequestID, &payment.Currency,
			&payment.Provider, &payment.Amount, &payment.PaymentDT, &payment.Bank,
			&payment.DeliveryCost, &payment.GoodsTotal, &payment.CustomFee,
			
			&itm.ChrtID, &itm.TrackNumber, &itm.Price, &itm.Rid,
			&itm.Name, &itm.Sale, &itm.Size, &itm.TotalPrice,
			&itm.NmID, &itm.Brand, &itm.Status,
		)
		if err == sql.ErrNoRows {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		
		items = append(items, itm)
		
		if flag == false {
			order.Delivery = delivery
			order.Payment = payment
			flag = true
		}
	}

	if err == sql.ErrNoRows {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		log.Println("getOrderHandler error:", err)
		return
	}
	orderJson, err := json.Marshal(order)
	if err != nil {
		http.Error(w, "json serialize failed", http.StatusInternalServerError)
	}
	a.Cache.Put(orderID, orderJson)
	
	w.Header().Set("Content-Type", "application/json")
	w.Write(orderJson)
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
		return fmt.Errorf("Warmup query failed: %w", err)
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
	
	mux := http.NewServeMux()
	mux.HandleFunc("GET /order/{id}", app.getOrderHandler)
	mux.Handle("/", http.FileServer(http.Dir("./static")))
	handler := loggingMiddleware(mux)
	srv := &http.Server{
		Addr:    ":8080",
		Handler: handler,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go app.startKafkaConsumer(ctx)

	go func() {
		log.Println("Server starting on ", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("ListenAndServe error: %v", err)
		}
	}()

	<-ctx.Done()
	stop()
	log.Println("Shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Server Shutdown Failed:%+v", err)
	}
}
