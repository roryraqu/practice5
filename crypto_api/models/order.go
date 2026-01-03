package models

type Order struct {
	PK       string
	UserID   string
	PairID   string
	Quantity float64
	Price    float64
	Type     string
	Closed   string
}