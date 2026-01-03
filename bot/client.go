package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"time"
)

const ExchangeURL = "http://localhost:80"

type UserResponse struct { Key string `json:"key"` }
type Lot struct { LotID int `json:"lot_id"`; Name string `json:"name"` }
type Pair struct { PairID int `json:"pair_id"`; SaleLotID string `json:"sale_lot_id"`; BuyLotID string `json:"buy_lot_id"` }
type Balance struct { LotID string `json:"lot_id"`; Total float64 `json:"total"`; Free float64 `json:"free"`; Reserved float64 `json:"reserved"` }
type Order struct { OrderID string `json:"order_id"`; PairID string `json:"pair_id"`; Type string `json:"type"`; Price json.Number `json:"price"`; Quantity json.Number `json:"quantity"`; Closed string `json:"closed"` }
type CreateOrderRequest struct { PairID int `json:"pair_id"`; Quantity float64 `json:"quantity"`; Price float64 `json:"price"`; Type string `json:"type"` }

type ExchangeClient struct {
	BaseURL string
	Key     string
	HTTP    *http.Client
}

func NewClient(url string) *ExchangeClient {
	return &ExchangeClient{
		BaseURL: url,
		HTTP:    &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *ExchangeClient) CreateUser(username string) error {
	for i := 0; i < 10; i++ {
		payload := map[string]string{"username": username}
		body, _ := json.Marshal(payload)
		resp, err := c.HTTP.Post(c.BaseURL+"/user", "application/json", bytes.NewBuffer(body))
		
		if err != nil || resp.StatusCode != 200 {
			if resp != nil { resp.Body.Close() }
			time.Sleep(time.Duration(rand.Intn(2000)+1000) * time.Millisecond)
			continue
		}
		var res UserResponse
		if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
			resp.Body.Close(); return err
		}
		resp.Body.Close()
		c.Key = res.Key
		return nil
	}
	return fmt.Errorf("failed to register")
}

func (c *ExchangeClient) request(method, endpoint string, payload interface{}) (*http.Response, error) {
	var body []byte
	if payload != nil { body, _ = json.Marshal(payload) }
	req, err := http.NewRequest(method, c.BaseURL+endpoint, bytes.NewBuffer(body))
	if err != nil { return nil, err }
	if c.Key != "" { req.Header.Set("X-USER-KEY", c.Key) }
	return c.HTTP.Do(req)
}

func (c *ExchangeClient) GetLots() ([]Lot, error) {
	r, e := c.request("GET", "/lot", nil); if e!=nil{return nil,e}; defer r.Body.Close(); var d []Lot; json.NewDecoder(r.Body).Decode(&d); return d, nil
}
func (c *ExchangeClient) GetPairs() ([]Pair, error) {
	r, e := c.request("GET", "/pair", nil); if e!=nil{return nil,e}; defer r.Body.Close(); var d []Pair; json.NewDecoder(r.Body).Decode(&d); return d, nil
}
func (c *ExchangeClient) GetBalance() ([]Balance, error) {
	r, e := c.request("GET", "/balance", nil); if e!=nil{return nil,e}; defer r.Body.Close(); var d []Balance; json.NewDecoder(r.Body).Decode(&d); return d, nil
}
func (c *ExchangeClient) GetAllOrders() ([]Order, error) {
	r, e := c.request("GET", "/order", nil); if e!=nil{return nil,e}; defer r.Body.Close(); var d []Order; json.NewDecoder(r.Body).Decode(&d); return d, nil
}
func (c *ExchangeClient) PlaceOrder(pid int, qty, price float64, t string) (string, error) {
	req := CreateOrderRequest{PairID: pid, Quantity: qty, Price: price, Type: t}
	r, e := c.request("POST", "/order", req); if e!=nil{return "",e}; defer r.Body.Close()
	if r.StatusCode != 200 { return "", fmt.Errorf("stat %d", r.StatusCode) }
	var res map[string]interface{}; json.NewDecoder(r.Body).Decode(&res)
	if oid, ok := res["order_id"].(string); ok { return oid, nil }; return "-1", nil
}
func (c *ExchangeClient) CancelOrder(oid string) error {
	_, e := c.request("DELETE", "/order", map[string]string{"order_id": oid}); return e
}