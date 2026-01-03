package handlers

import (
	"strconv"
	"crypto_api/utils"
	"crypto_api/models"
	"crypto_api/business_logic"
	"crypto_api/db"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

func getBalance(userID, lotID string) (float64, error) {
	resp, err := db.ExecQuery(fmt.Sprintf(
		"SELECT user_lot.quantity FROM user_lot WHERE user_lot.user_id = '%s' AND user_lot.lot_id = '%s'",
		userID, lotID))
	if err != nil || resp == "" || strings.Contains(resp, "0 rows selected") {
		return 0.0, nil
	}
	parts := utils.CleanLine(resp)
	if len(parts) == 0 {
		return 0.0, nil
	}
	return strconv.ParseFloat(parts[0], 64)
}

type CreateOrderRequest struct {
	PairID   int     `json:"pair_id"`
	Quantity float64 `json:"quantity"`
	Price    float64 `json:"price"`
	Type     string  `json:"type"`
}

func PostOrder(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("X-USER-KEY")
	if key == "" {
		http.Error(w, "Missing X-USER-KEY", http.StatusUnauthorized)
		return
	}

	var req CreateOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if req.Type != "buy" && req.Type != "sell" {
		http.Error(w, "type must be 'buy' or 'sell'", http.StatusBadRequest)
		return
	}
	if req.Quantity <= 0 || req.Price <= 0 {
		http.Error(w, "quantity and price must be > 0", http.StatusBadRequest)
		return
	}

	resp, err := db.ExecQuery(fmt.Sprintf(
		"SELECT user.user_pk FROM user WHERE user.key = '%s'", key))
	if err != nil || resp == "" {
		http.Error(w, "Invalid user key", http.StatusUnauthorized)
		return
	}
	userID := utils.CleanLine(resp)[0]

	pairIDStr := strconv.Itoa(req.PairID)
	pairResp, err := db.ExecQuery(fmt.Sprintf(
		"SELECT pair.first_lot_id, pair.second_lot_id FROM pair WHERE pair.pair_pk = '%s'", pairIDStr))
	if err != nil || pairResp == "" || strings.Contains(pairResp, "0 rows selected") {
		http.Error(w, "Pair not found", http.StatusBadRequest)
		return
	}
	pairParts := utils.CleanLine(pairResp)
	if len(pairParts) < 2 {
		http.Error(w, "Invalid pair data", http.StatusInternalServerError)
		return
	}
	saleLot := pairParts[0]
	payLot := pairParts[1]

	var reserveLot string
	var reserveAmount float64
	if req.Type == "buy" {
		reserveLot = payLot
		reserveAmount = req.Quantity * req.Price
	} else {
		reserveLot = saleLot
		reserveAmount = req.Quantity
	}

	currentTotal, _ := getBalance(userID, reserveLot)
	currentHold, _ := business_logic.GetHold(userID, reserveLot)
	freeBalance := currentTotal - currentHold

	if freeBalance < reserveAmount-1e-9 {
		http.Error(w, "Insufficient balance", http.StatusBadRequest)
		return
	}

	err = business_logic.SetHold(userID, reserveLot, currentHold+reserveAmount)
	if err != nil {
		http.Error(w, "Reserve failed", http.StatusInternalServerError)
		return
	}

	newOrder := models.Order{
		UserID:   userID,
		PairID:   pairIDStr,
		Quantity: req.Quantity,
		Price:    req.Price,
		Type:     req.Type,
	}

	orderID, err := business_logic.MatchOrder(newOrder)
	if err != nil {
		business_logic.ReleaseHold(userID, reserveLot, reserveAmount)
		http.Error(w, "Matching error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if orderID == "-1" {
		json.NewEncoder(w).Encode(map[string]interface{}{"order_id": nil})
	} else {
		json.NewEncoder(w).Encode(map[string]interface{}{"order_id": orderID})
	}
}

type DeleteOrderRequest struct {
	OrderID string `json:"order_id"`
}

func DeleteOrder(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("X-USER-KEY")
	if key == "" {
		http.Error(w, "Missing X-USER-KEY", http.StatusUnauthorized)
		return
	}

	var req DeleteOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	if req.OrderID == "" {
		http.Error(w, "Missing order_id", http.StatusBadRequest)
		return
	}

	resp, err := db.ExecQuery(fmt.Sprintf(
		"SELECT order.user_id, order.pair_id, order.quantity, order.price, order.type "+
			"FROM order WHERE order.order_pk = '%s' AND order.closed = ''", req.OrderID))
	if err != nil || resp == "" || strings.Contains(resp, "0 rows selected") {
		http.Error(w, "Order not found or closed", http.StatusNotFound)
		return
	}
	orderParts := utils.CleanLine(resp)
	if len(orderParts) < 5 {
		http.Error(w, "Invalid order", http.StatusInternalServerError)
		return
	}
	orderUserID := orderParts[0]
	pairID := orderParts[1]
	qty, _ := strconv.ParseFloat(orderParts[2], 64)
	price, _ := strconv.ParseFloat(orderParts[3], 64)
	orderType := orderParts[4]

	resp, err = db.ExecQuery(fmt.Sprintf(
		"SELECT user.user_pk FROM user WHERE user.key = '%s'", key))
	if err != nil || resp == "" {
		http.Error(w, "Invalid key", http.StatusUnauthorized)
		return
	}
	userID := utils.CleanLine(resp)[0]
	if orderUserID != userID {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	pairResp, err := db.ExecQuery(fmt.Sprintf(
		"SELECT pair.first_lot_id, pair.second_lot_id FROM pair WHERE pair.pair_pk = '%s'", pairID))
	if err != nil || pairResp == "" {
		http.Error(w, "Pair error", http.StatusInternalServerError)
		return
	}
	pairParts := utils.CleanLine(pairResp)
	if len(pairParts) < 2 {
		http.Error(w, "Pair data error", http.StatusInternalServerError)
		return
	}
	saleLot := pairParts[0]
	payLot := pairParts[1]

	var releaseLot string
	var releaseAmount float64
	if orderType == "buy" {
		releaseLot = payLot
		releaseAmount = qty * price
	} else {
		releaseLot = saleLot
		releaseAmount = qty
	}
	business_logic.ReleaseHold(userID, releaseLot, releaseAmount)

	db.ExecQuery(fmt.Sprintf("DELETE FROM order WHERE order.order_pk = '%s'", req.OrderID))

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"status": "deleted"})
}

func GetOrders(w http.ResponseWriter, r *http.Request) {
	resp, err := db.ExecQuery("SELECT order.order_pk, order.user_id, order.pair_id, order.quantity, order.price, order.type, order.closed FROM order")
	if err != nil {
		http.Error(w, "DB error", http.StatusInternalServerError)
		return
	}

	lines := strings.Split(resp, "\n")
	var orders []map[string]interface{}

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := utils.CleanLine(line)

		if len(parts) < 6 {
			continue
		}

		closed := ""
		if len(parts) > 6 {
			closed = parts[6]
		}

		orders = append(orders, map[string]interface{}{
			"order_id":  parts[0],
			"user_id":   parts[1],
			"pair_id":   parts[2],
			"quantity":  parts[3],
			"price":     parts[4],
			"type":      parts[5],
			"closed":    closed,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(orders)
}