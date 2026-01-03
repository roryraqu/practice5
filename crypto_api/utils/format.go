package utils

import (
	"strings"
	"fmt"
)

func CleanLine(line string) []string {
	line = strings.TrimRight(line, " \r\n|")
	parts := strings.Split(line, " | ")
	var clean []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" && p != "|" {
			clean = append(clean, p)
		}
	}
	return clean
}

func FormatNumber(x float64) string {
	s := fmt.Sprintf("%.10f", x)
	s = strings.TrimRight(s, "0")
	s = strings.TrimRight(s, ".")
	if s == "" {
		return "0"
	}
	return s
}