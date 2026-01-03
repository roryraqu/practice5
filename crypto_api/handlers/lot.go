package handlers

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"crypto_api/db"
)

func GetLots(w http.ResponseWriter, r *http.Request) {
	resp, err := db.ExecQuery("SELECT lot.lot_pk, lot.name FROM lot")
	if err != nil {
		http.Error(w, "DB error", http.StatusInternalServerError)
		return
	}

	lines := strings.Split(resp, "\n")
	type Lot struct {
		LotID int    `json:"lot_id"`
		Name  string `json:"name"`
	}
	var lots []Lot

	for _, line := range lines[0:] {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Split(line, " | ")
		if len(parts) < 2 {
			continue
		}
		lotID, err := strconv.Atoi(parts[0])
		if err != nil {
			continue
		}
		lots = append(lots, Lot{
			LotID: lotID,
			Name:  parts[1],
		})
	}

	sort.Slice(lots, func(i, j int) bool {
		return lots[i].LotID < lots[j].LotID
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(lots)
}