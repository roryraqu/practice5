package main

import (
	"crypto_api/db"
	"crypto_api/handlers"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
)

type Config struct {
	Lots         []string `json:"lots"`
	DatabaseIP   string   `json:"database_ip"`
	DatabasePort int      `json:"database_port"`
}

func main() {
	data, err := os.ReadFile("config.json")
	if err != nil {
		panic("config.json not found")
	}
	var cfg Config
	json.Unmarshal(data, &cfg)

	db.DBAddr = fmt.Sprintf("%s:%d", cfg.DatabaseIP, cfg.DatabasePort)

	fmt.Println("Connecting to DB...")
	if err := db.InitConnection(); err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
	}
	fmt.Println("Connected.")

	status := db.InitDatabase(cfg.Lots)
	if status == 1 {
		log.Println("DB Init failed")
		return
	}

	http.HandleFunc("/user", handlers.PostUser)
	http.HandleFunc("/lot", handlers.GetLots)
	http.HandleFunc("/pair", handlers.GetPairs)
	http.HandleFunc("/balance", handlers.GetBalance)

	http.HandleFunc("/order", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "POST":
			handlers.PostOrder(w, r)
		case "GET":
			handlers.GetOrders(w, r)
		case "DELETE":
			handlers.DeleteOrder(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	fmt.Println("API listening on :8000")
	log.Fatal(http.ListenAndServe(":8000", nil))
}