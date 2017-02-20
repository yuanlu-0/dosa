package main

import (
	"context"
	"fmt"
	"github.com/uber-go/dosa"
	_ "github.com/uber-go/dosa/connectors/cassandra"
)

type Book struct {
	dosa.Entity `dosa:"primaryKey=ISBN"`
	ISBN        string
	Price       float64
}

func main() {
	client, _ := dosa.NewClient("cassandra", &Book{})

	ctx := context.Background()
	client.Initialize(ctx)
	err := client.Upsert(ctx, nil, &Book{ISBN: "abc", Price: 23.45})
	if err != nil {
		fmt.Printf("err = %s\n", err)
	}

}
