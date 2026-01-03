package db

import (
	"fmt"
	"strings"
)

func InitDatabase(lots []string) int {
	if err := InitConnection(); err != nil {
		fmt.Printf("Failed to connect to DB at %s: %v\n", DBAddr, err)
		return 1
	}

	resp, err := ExecQuery("SELECT lot.name FROM lot")
	if err != nil {
		fmt.Println("Warning: failed to check existing lots (DB may be empty or error)")
	}

	if resp != "" && !strings.Contains(resp, "0 rows selected") {
		lines := strings.Split(resp, "\n")
		if len(lines) > 0 && !strings.Contains(lines[0], "Error") {
			fmt.Println("Lots already exist — skipping initialization")
			return 0
		}
	}

	fmt.Println("Initializing lots and pairs...")

	lotIDs := make(map[string]string)

	for _, lotName := range lots {
		query := fmt.Sprintf("INSERT INTO lot VALUES('%s')", lotName)
		_, err := ExecQuery(query)
		if err != nil {
			fmt.Printf("Failed to insert lot '%s': %v\n", lotName, err)
			return 1
		}

		resp, err := ExecQuery(fmt.Sprintf("SELECT lot.lot_pk FROM lot WHERE lot.name = '%s'", lotName))
		if err != nil || resp == "" {
			fmt.Printf("Failed to get pk for lot '%s'\n", lotName)
			return 1
		}
		lines := strings.Split(resp, "\n")
		pk := strings.TrimSpace(strings.Split(lines[0], "|")[0])
		lotIDs[lotName] = pk
	}

	pairCount := 0
	for _, sale := range lots {
		for _, buy := range lots {
			if sale == buy {
				continue
			}
			query := fmt.Sprintf(
				"INSERT INTO pair VALUES('%s','%s')",
				lotIDs[sale], lotIDs[buy],
			)
			_, err := ExecQuery(query)
			if err != nil {
				fmt.Printf("Failed to insert pair %s/%s: %v\n", sale, buy, err)
				return 1
			} else {
				pairCount++
			}
		}
	}

	fmt.Printf("Initialized %d lots and %d pairs\n", len(lots), pairCount)
	return 0
}