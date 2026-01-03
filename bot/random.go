package main

import (
	"fmt"
	"math/rand"
	"time"
)

var RealPricesRandom = map[string]float64{
	"BTC": 9500000.0, "ETH": 260000.0, "SOL": 14500.0, "DOGE": 35.0,
	"USDT": 100.0, "USDC": 100.0, "RUB": 1.0,
}

func main() {
	rand.Seed(time.Now().UnixNano())
	client := NewClient(ExchangeURL)
	botName := fmt.Sprintf("Random_%d", rand.Intn(90000)+10000)

	if err := client.CreateUser(botName); err != nil {
		fmt.Printf("Bot %s failed: %v\n", botName, err)
		return
	}
	fmt.Printf("Bot %s started\n", botName)

	lots, _ := client.GetLots()
	pairs, _ := client.GetPairs()

	lotMap := make(map[string]string)
	for _, l := range lots {
		lotMap[fmt.Sprintf("%d", l.LotID)] = l.Name
	}

	for {
		time.Sleep(time.Duration(rand.Intn(1000)+500) * time.Millisecond)

		if len(pairs) == 0 {
			pairs, _ = client.GetPairs()
			continue
		}

		p := pairs[rand.Intn(len(pairs))]
		saleName := lotMap[p.SaleLotID]
		buyName := lotMap[p.BuyLotID]

		pSale := RealPricesRandom[saleName]
		pBuy := RealPricesRandom[buyName]
		if pSale == 0 { pSale = 100.0 }
		if pBuy == 0 { pBuy = 1.0 }

		fairPrice := pSale / pBuy
		price := fairPrice * (0.98 + rand.Float64()*0.04)

		balances, err := client.GetBalance()
		if err != nil { continue }

		var qtyAsset, qtyMoney float64
		for _, b := range balances {
			if b.LotID == p.SaleLotID { qtyAsset = b.Free }
			if b.LotID == p.BuyLotID { qtyMoney = b.Free }
		}

		action := ""
		if qtyMoney < 10.0 && qtyAsset > 0.001 {
			action = "sell"
			price = fairPrice * 0.95
		} else if qtyAsset < 0.001 && qtyMoney > 10.0 {
			action = "buy"
			price = fairPrice * 1.05
		} else {
			if rand.Intn(2) == 0 { action = "buy" } else { action = "sell" }
		}

		var qty float64
		if action == "buy" {
			if qtyMoney < 1.0 { continue }
			qty = (qtyMoney * 0.2) / price
		} else {
			if qtyAsset < 0.0001 { continue }
			qty = qtyAsset * 0.2
		}

		if qty > 0.000001 {
			client.PlaceOrder(p.PairID, qty, price, action)
		}
	}
}