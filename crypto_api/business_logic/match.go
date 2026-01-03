package business_logic

import (
	"crypto_api/db"
	"crypto_api/models"
	"crypto_api/utils"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

func adjustBalance(userID, lotID string, amount float64, add bool) error {
	resp, err := db.ExecQuery(fmt.Sprintf(
		"SELECT user_lot.user_lot_pk, user_lot.quantity FROM user_lot WHERE user_lot.user_id = '%s' AND user_lot.lot_id = '%s'",
		userID, lotID))

	var pk string
	var currentQty float64 = 0

	if err == nil && resp != "" && !strings.Contains(resp, "0 rows selected") {
		lines := strings.Split(strings.TrimSpace(resp), "\n")
		lastLine := lines[len(lines)-1]
		parts := utils.CleanLine(lastLine)
		if len(parts) >= 2 {
			pk = parts[0]
			currentQty, _ = strconv.ParseFloat(parts[1], 64)
		}
	}

	newQty := currentQty
	if add {
		newQty += amount
	} else {
		newQty -= amount
	}

	newQty = math.Round(newQty*1e10) / 1e10
	if newQty < 0 {
		newQty = 0
	}

	if pk != "" {
		db.ExecQuery(fmt.Sprintf("DELETE FROM user_lot WHERE user_lot.user_lot_pk = '%s'", pk))
	}
	db.ExecQuery(fmt.Sprintf("INSERT INTO user_lot VALUES('%s','%s','%.10f')", userID, lotID, newQty))
	return nil
}

func GetHold(userID, lotID string) (float64, error) {
	resp, err := db.ExecQuery(fmt.Sprintf(
		"SELECT hold.amount FROM hold WHERE hold.user_id = '%s' AND hold.lot_id = '%s'",
		userID, lotID))
	if err != nil || resp == "" || strings.Contains(resp, "0 rows selected") {
		return 0.0, nil
	}
	lines := strings.Split(strings.TrimSpace(resp), "\n")
	parts := utils.CleanLine(lines[len(lines)-1])
	if len(parts) == 0 {
		return 0.0, nil
	}
	return strconv.ParseFloat(parts[0], 64)
}

func SetHold(userID, lotID string, amount float64) error {
	db.ExecQuery(fmt.Sprintf("DELETE FROM hold WHERE hold.user_id = '%s' AND hold.lot_id = '%s'", userID, lotID))

	if amount > 1e-9 {
		db.ExecQuery(fmt.Sprintf("INSERT INTO hold VALUES('%s','%s','%.10f')", userID, lotID, amount))
	}
	return nil
}

func ReleaseHold(userID, lotID string, amount float64) error {
	current, _ := GetHold(userID, lotID)
	newHold := current - amount
	if newHold < -1e-9 {
		newHold = 0
	}
	return SetHold(userID, lotID, newHold)
}

func MatchOrder(newOrder models.Order) (string, error) {
	pairResp, err := db.ExecQuery(fmt.Sprintf(
		"SELECT pair.pair_pk, pair.first_lot_id, pair.second_lot_id FROM pair WHERE pair.pair_pk = '%s'",
		newOrder.PairID))
	if err != nil || pairResp == "" {
		return "", fmt.Errorf("pair not found")
	}
	pairParts := utils.CleanLine(pairResp)
	if len(pairParts) < 3 {
		return "", fmt.Errorf("invalid pair data")
	}
	saleLotID := pairParts[1]
	payLotID := pairParts[2]

	isBuy := newOrder.Type == "buy"
	oppositeType := "sell"
	if !isBuy {
		oppositeType = "buy"
	}

	query := fmt.Sprintf(
		"SELECT order.order_pk, order.user_id, order.quantity, order.price "+
			"FROM order WHERE order.pair_id = '%s' AND order.type = '%s' AND order.closed = ''",
		newOrder.PairID, oppositeType)
	resp, err := db.ExecQuery(query)
	if err != nil {
		return "", fmt.Errorf("DB query failed: %w", err)
	}

	var oppositeOrders []models.Order
	if resp != "" {
		lines := strings.Split(resp, "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" { continue }
			parts := utils.CleanLine(line)
			if len(parts) < 4 { continue }
			qty, _ := strconv.ParseFloat(parts[2], 64)
			price, _ := strconv.ParseFloat(parts[3], 64)
			oppositeOrders = append(oppositeOrders, models.Order{
				PK: parts[0], UserID: parts[1], Quantity: qty, Price: price, Type: oppositeType,
			})
		}
	}

	var filteredOrders []models.Order
	for _, opp := range oppositeOrders {
		if isBuy && opp.Price <= newOrder.Price+1e-9 {
			filteredOrders = append(filteredOrders, opp)
		} else if !isBuy && opp.Price >= newOrder.Price-1e-9 {
			filteredOrders = append(filteredOrders, opp)
		}
	}

	if isBuy {
		sort.Slice(filteredOrders, func(i, j int) bool { return filteredOrders[i].Price < filteredOrders[j].Price })
	} else {
		sort.Slice(filteredOrders, func(i, j int) bool { return filteredOrders[i].Price > filteredOrders[j].Price })
	}

	remainingQty := newOrder.Quantity
	for _, opp := range filteredOrders {
		if remainingQty <= 1e-9 { break }
		execQty := math.Min(remainingQty, opp.Quantity)
		execPrice := opp.Price

		if isBuy {
			ReleaseHold(newOrder.UserID, payLotID, execQty*execPrice)
			ReleaseHold(opp.UserID, saleLotID, execQty)
		} else {
			ReleaseHold(newOrder.UserID, saleLotID, execQty)
			ReleaseHold(opp.UserID, payLotID, execQty*execPrice)
		}

		if isBuy {
			adjustBalance(newOrder.UserID, saleLotID, execQty, true)
			adjustBalance(newOrder.UserID, payLotID, execQty*execPrice, false)
			adjustBalance(opp.UserID, saleLotID, execQty, false)
			adjustBalance(opp.UserID, payLotID, execQty*execPrice, true)
		} else {
			adjustBalance(newOrder.UserID, saleLotID, execQty, false)
			adjustBalance(newOrder.UserID, payLotID, execQty*execPrice, true)
			adjustBalance(opp.UserID, saleLotID, execQty, true)
			adjustBalance(opp.UserID, payLotID, execQty*execPrice, false)
		}

		db.ExecQuery(fmt.Sprintf("DELETE FROM order WHERE order.order_pk = '%s'", opp.PK))

		if opp.Quantity-execQty > 1e-9 {
			remQty := opp.Quantity - execQty
			fmtQty := utils.FormatNumber(remQty)
			fmtPrice := utils.FormatNumber(opp.Price)
			db.ExecQuery(fmt.Sprintf(
				"INSERT INTO order VALUES('%s','%s','%s','%s','%s','')",
				opp.UserID, newOrder.PairID, fmtQty, fmtPrice, opp.Type))
		} else {
			ts := fmt.Sprintf("%d", time.Now().Unix())
			fmtQty := utils.FormatNumber(opp.Quantity)
			fmtPrice := utils.FormatNumber(opp.Price)
			db.ExecQuery(fmt.Sprintf(
				"INSERT INTO order VALUES('%s','%s','%s','%s','%s','%s')",
				opp.UserID, newOrder.PairID, fmtQty, fmtPrice, opp.Type, ts))
		}
		remainingQty -= execQty
	}

	if remainingQty > 1e-9 {
		fmtQty := utils.FormatNumber(remainingQty)
		fmtPrice := utils.FormatNumber(newOrder.Price)
		db.ExecQuery(fmt.Sprintf(
			"INSERT INTO order VALUES('%s','%s','%s','%s','%s','')",
			newOrder.UserID, newOrder.PairID, fmtQty, fmtPrice, newOrder.Type))

		selectQuery := fmt.Sprintf(
			"SELECT order.order_pk FROM order WHERE order.user_id = '%s' AND order.pair_id = '%s' AND order.quantity = '%s' AND order.price = '%s' AND order.type = '%s' AND order.closed = ''",
			newOrder.UserID, newOrder.PairID, fmtQty, fmtPrice, newOrder.Type)
		resp, err := db.ExecQuery(selectQuery)
		if err != nil || resp == "" { return "", fmt.Errorf("failed to retrieve order_id") }
		lines := strings.Split(strings.TrimSpace(resp), "\n")
		parts := utils.CleanLine(lines[len(lines)-1])
		if len(parts) == 0 { return "", fmt.Errorf("empty order_id response") }
		return parts[0], nil
	} else {
		fmtQty := utils.FormatNumber(newOrder.Quantity)
		fmtPrice := utils.FormatNumber(newOrder.Price)
		ts := fmt.Sprintf("%d", time.Now().Unix())
		db.ExecQuery(fmt.Sprintf(
			"INSERT INTO order VALUES('%s','%s','%s','%s','%s','%s')",
			newOrder.UserID, newOrder.PairID, fmtQty, fmtPrice, newOrder.Type, ts))
		return "-1", nil
	}
}