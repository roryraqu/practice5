package handlers

import (
	"crypto_api/db"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

func GetPairs(w http.ResponseWriter, r *http.Request) {
	resp, err := db.ExecQuery("SELECT pair.pair_pk, pair.first_lot_id, pair.second_lot_id FROM pair")
	if err != nil {
		http.Error(w, "DB error", http.StatusInternalServerError)
		return
	}

	lines := strings.Split(resp, "\n")
	type Pair struct {
		PairID     int    `json:"pair_id"`
		SaleLotID  string `json:"sale_lot_id"`
		BuyLotID   string `json:"buy_lot_id"`
	}
	var pairs []Pair

	for _, line := range lines[1:] {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Split(line, " | ")
		if len(parts) < 3 {
			continue
		}
		pairID, _ := strconv.Atoi(parts[0])
		pairs = append(pairs, Pair{
			PairID:    pairID,
			SaleLotID: parts[1],
			BuyLotID:  parts[2],
		})
	}

	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].PairID < pairs[j].PairID
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(pairs)
}