package main

import (
	"fmt"
	"log"
	"sort"
	"strconv"
	"time"
)

const RoundDuration = 100.0

var RealPrices = map[string]float64{
	"BTC": 9500000.0, "ETH": 260000.0, "SOL": 14500.0, "DOGE": 35.0,
	"USDT": 100.0, "USDC": 100.0, "RUB": 1.0,
}

type MyOrder struct {
	ID        string
	Timestamp float64
}

func main() {
	log.SetFlags(log.Ltime)
	client := NewClient(ExchangeURL)
	botName := fmt.Sprintf("Smart_%d", time.Now().Unix())

	if err := client.CreateUser(botName); err != nil {
		log.Fatal("Ошибка регистрации")
	}
	log.Printf("Запуск бота %s", botName)

	lots, err := client.GetLots()
	if err != nil {
		log.Fatal("Не удалось загрузить лоты")
	}
	pairs, err := client.GetPairs()
	if err != nil {
		log.Fatal("Не удалось загрузить пары")
	}

	rubID := "1"
	for _, l := range lots {
		if l.Name == "RUB" {
			rubID = fmt.Sprintf("%d", l.LotID)
			break
		}
	}

	lotNames := make(map[string]string)
	for _, l := range lots {
		lotNames[fmt.Sprintf("%d", l.LotID)] = l.Name
	}

	sellPairs := make(map[string]Pair)
	for _, p := range pairs {
		if p.BuyLotID == rubID {
			sellPairs[p.SaleLotID] = p
		}
	}

	startTime := time.Now()
	var myOrders []MyOrder
	var currentGreed string

	for {
		elapsed := time.Since(startTime).Seconds()
		remaining := RoundDuration - elapsed
		if remaining <= 0 {
			log.Println("Время вышло")
			break
		}

		var greed float64
		var txnAmount float64
		var strategy string

		if remaining > 60 {
			greed = 0.95
			txnAmount = 200.0
			strategy = "Жадная (0.95)"
		} else if remaining > 30 {
			greed = 0.50
			txnAmount = 100.0
			strategy = "Сброс (0.50)"
		} else {
			greed = 0.01
			txnAmount = 50.0
			strategy = "Паника (0.01)"
		}

		if strategy != currentGreed {
			log.Printf("Стратегия: %s. Осталось: %ds", strategy, int(remaining))
			currentGreed = strategy
		}

		now := float64(time.Now().UnixNano()) / 1e9
		var active []MyOrder
		for _, o := range myOrders {
			if now-o.Timestamp > 2.0 {
				client.CancelOrder(o.ID)
			} else {
				active = append(active, o)
			}
		}
		myOrders = active

		balances, err := client.GetBalance()
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		allOrders, err := client.GetAllOrders()
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		bestBids := make(map[int]float64)
		for _, o := range allOrders {
			if o.Closed != "" || o.Type != "buy" {
				continue
			}
			pairID, err := strconv.Atoi(o.PairID)
			if err != nil {
				continue
			}
			price, err := o.Price.Float64()
			if err != nil || price <= 0 {
				continue
			}
			if price > bestBids[pairID] {
				bestBids[pairID] = price
			}
		}

		type BalanceWithLot struct {
			LotID string
			Free  float64
		}
		var balList []BalanceWithLot
		for _, b := range balances {
			if b.LotID != rubID && b.Free > 0.000001 {
				balList = append(balList, BalanceWithLot{b.LotID, b.Free})
			}
		}
		sort.Slice(balList, func(i, j int) bool {
			return balList[i].LotID < balList[j].LotID
		})

		for _, bal := range balList {
			if pair, ok := sellPairs[bal.LotID]; ok {
				lotName := lotNames[bal.LotID]
				fairPrice, ok := RealPrices[lotName]
				if !ok {
					fairPrice = 100.0
				}

				bestBid, hasMarket := bestBids[pair.PairID]

				if hasMarket && (bestBid > fairPrice*0.1 || remaining < 30) {
					maxVol := bal.Free
					if maxVol*bestBid > 500.0 {
						maxVol = 500.0 / bestBid
					}
					if maxVol > 0.000001 {
						oid, err := client.PlaceOrder(pair.PairID, maxVol, bestBid, "sell")
						if err == nil && oid != "" && oid != "-1" {
							time.Sleep(50 * time.Millisecond)
							continue
						}
					}
				}

				targetPrice := fairPrice * greed
				vol := txnAmount / targetPrice
				sellQty := bal.Free
				if vol < sellQty {
					sellQty = vol
				}
				if sellQty > 0.000001 {
					oid, err := client.PlaceOrder(pair.PairID, sellQty, targetPrice, "sell")
					if err == nil && oid != "" && oid != "-1" {
						myOrders = append(myOrders, MyOrder{ID: oid, Timestamp: now})
					}
				}
			}
		}

		time.Sleep(500 * time.Millisecond)
	}

	for _, o := range myOrders {
		client.CancelOrder(o.ID)
	}
	time.Sleep(2 * time.Second)

	finalBalances, _ := client.GetBalance()
	startRub := 1000.0
	var rubBalance float64
	for _, b := range finalBalances {
		if b.LotID == rubID {
			rubBalance = b.Total
			break
		}
	}
	profit := rubBalance - startRub
	fmt.Printf("\nИтог: %.2f RUB (Прибыль: %.2f)\n", rubBalance, profit)
}