package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/hirokisan/bybit/v2"
)

type Creds struct {
	APIKey    string `json:"api_key"`
	APISecret string `json:"api_secret"`
}

func main() {
	raw, _ := os.ReadFile("credentials/bybit.json")
	var c Creds
	json.Unmarshal(raw, &c)

	client := bybit.NewTestClient().WithAuth(c.APIKey, c.APISecret)

	resp, err := client.V5().Account().GetAccountInfo()
	if err != nil {
		log.Fatalf("GetAccountInfo failed: %v", err)
	}

	fmt.Printf("Account Status: %v\n", resp.Result.UnifiedMarginStatus)

	// Check balance again
	bal, err := client.V5().Account().GetWalletBalance(bybit.AccountTypeV5UNIFIED, nil)
	if err != nil {
		fmt.Printf("GetWalletBalance (UNIFIED) failed: %v\n", err)
	} else {
		fmt.Println("Account is UNIFIED type.")
		for _, l := range bal.Result.List {
			for _, co := range l.Coin {
				valStr := co.Equity
				if valStr == "" || valStr == "0" {
					valStr = co.WalletBalance
				}
				if valStr == "" || valStr == "0" {
					valStr = co.AvailableToWithdraw
				}
				fmt.Printf("Coin: %s, Best Val: %s (Equity: %s, Wallet: %s, Avail: %s)\n",
					co.Coin, valStr, co.Equity, co.WalletBalance, co.AvailableToWithdraw)
			}
		}
	}

	// Test Private WS Auth
	wsClient := bybit.NewWebsocketClient().
		WithBaseURL(bybit.TestWebsocketBaseURL).
		WithAuth(c.APIKey, c.APISecret)

	priv, _ := wsClient.V5().Private()
	err = priv.Start(context.Background(), func(isClosed bool, err error) {
		if err != nil {
			fmt.Printf("WS Handler Result: closed=%v, err=%v\n", isClosed, err)
		}
	})
	if err != nil {
		fmt.Printf("WS Start failed: %v\n", err)
	} else {
		fmt.Println("WS Start success (waiting 10s for auth/error result...)")
		time.Sleep(10 * time.Second)
	}
}
