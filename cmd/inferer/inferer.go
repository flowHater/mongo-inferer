package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/flowHater/mongo-inferer/pkg/discover"
	"github.com/flowHater/mongo-inferer/pkg/seeder"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatalf("An error occured during mongodb client's initialization")
	}

	r := discover.NewRepository(discover.RepositoryWithClient(client))
	d := discover.New(ctx, r)

	m, err := d.Database(ctx, seeder.Database)
	if err != nil {
		log.Fatalln(err)
	}
	jm, err := json.Marshal(m)
	fmt.Println(string(jm))
}
