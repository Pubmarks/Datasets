package main

import (
	"log"
)

func main() {
	log.SetFlags(0)
	log.SetPrefix("macrotrends: ")
	if err := newRootCmd().Execute(); err != nil {
		log.Fatal(err)
	}
}
