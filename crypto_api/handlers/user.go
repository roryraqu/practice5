package handlers

import (
	"crypto_api/db"
	"crypto_api/utils"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

type CreateUserRequest struct {
	Username string `json:"username"`
}

func PostUser(w http.ResponseWriter, r *http.Request) {
	var req CreateUserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if req.Username == "" {
		http.Error(w, "Username is required", http.StatusBadRequest)
		return
	}

	key, err := utils.GenerateUserKey()
	if err != nil {
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	query := fmt.Sprintf("INSERT INTO user VALUES('%s','%s')", req.Username, key)
	_, err = db.ExecQuery(query)
	if err != nil {
		http.Error(w, "Failed to create user", http.StatusInternalServerError)
		return
	}

	resp, err := db.ExecQuery(fmt.Sprintf("SELECT user.user_pk FROM user WHERE user.key = '%s'", key))
	if err != nil || resp == "" {
		http.Error(w, "Failed to get user_id", http.StatusInternalServerError)
		return
	}
	userID := strings.Split(resp, " | ")[0]

	resp, err = db.ExecQuery("SELECT lot.lot_pk FROM lot")
	if err != nil {
		http.Error(w, "Failed to load lots", http.StatusInternalServerError)
		return
	}

	lines := strings.Split(resp, "\n")
	for _, line := range lines[0:] {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Split(line, " | ")
		if len(parts) == 0 || parts[0] == "" {
			continue
		}
		lotID := parts[0]
		db.ExecQuery(fmt.Sprintf("INSERT INTO user_lot VALUES('%s','%s','1000')", userID, lotID))
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"key": key})
}