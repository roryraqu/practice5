package handlers

import (
	"crypto_api/db"
	"crypto_api/utils"
	"crypto_api/business_logic"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

func GetBalance(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("X-USER-KEY")
	if key == "" {
		http.Error(w, "Missing X-USER-KEY", http.StatusUnauthorized)
		return
	}

	resp, err := db.ExecQuery(fmt.Sprintf("SELECT user.user_pk FROM user WHERE user.key = '%s'", key))
	if err != nil || resp == "" {
		http.Error(w, "Invalid user key", http.StatusUnauthorized)
		return
	}
	userID := utils.CleanLine(resp)[0]

	lotsResp, err := db.ExecQuery("SELECT lot.lot_pk FROM lot")
	if err != nil {
		http.Error(w, "DB error", http.StatusInternalServerError)
		return
	}
	allLotIDs := []string{}
	if lotsResp != "" {
		for _, line := range strings.Split(lotsResp, "\n") {
			line = strings.TrimSpace(line)
			if line != "" {
				parts := utils.CleanLine(line)
				if len(parts) > 0 {
					allLotIDs = append(allLotIDs, parts[0])
				}
			}
		}
	}

	balances := make(map[string]float64)
	resp, err = db.ExecQuery(fmt.Sprintf(
		"SELECT user_lot.lot_id, user_lot.quantity FROM user_lot WHERE user_lot.user_id = '%s'", userID))
	if err == nil && resp != "" {
		for _, line := range strings.Split(resp, "\n") {
			line = strings.TrimSpace(line)
			if line != "" {
				parts := utils.CleanLine(line)
				if len(parts) >= 2 {
					lotID := parts[0]
					qty, _ := strconv.ParseFloat(parts[1], 64)
					balances[lotID] = qty
				}
			}
		}
	}

	result := []map[string]interface{}{}
	for _, lotID := range allLotIDs {
		total := balances[lotID]
		reserved, _ := business_logic.GetHold(userID, lotID)
		if reserved > total {
			reserved = total
		}
		free := total - reserved

		result = append(result, map[string]interface{}{
			"lot_id":    lotID,
			"total":     total,
			"free":      free,
			"reserved":  reserved,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		idI, _ := strconv.Atoi(result[i]["lot_id"].(string))
		idJ, _ := strconv.Atoi(result[j]["lot_id"].(string))
		return idI < idJ
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}