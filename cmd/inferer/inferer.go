package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/flowHater/mongo-inferer/pkg/seeder"
	"log"

	"github.com/flowHater/mongo-inferer/pkg/discover"
	"github.com/flowHater/mongo-inferer/pkg/repository"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatalf("An error occured during mongodb client's initialization")
	}

	r := repository.New(repository.WithClient(client))
	d := discover.New(r)

	m, _ := d.Collection(ctx, seeder.Database, seeder.CollectionA)

	jm, err := json.Marshal(m)
	fmt.Println(string(jm))
}
