package main

import (
	"context"
	"log"

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

	seeder.Seed(ctx, client)
}
