package main

import (
	"fmt"
)

func main() {
	sth := make(map[string]bool)
	sth["1"] = true
	fmt.Print(sth["1"])
}
