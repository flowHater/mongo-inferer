package seeder

import (
	"context"
	"fmt"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	Database    = "inferer-database-test"
	CollectionA = "A"
	CollectionB = "B"
	CollectionC = "C"
)

// Seed will fill the database with dummy data
func Seed(ctx context.Context, client *mongo.Client) {
	d := client.Database(Database)
	A := d.Collection(CollectionA)
	B := d.Collection(CollectionB)
	C := d.Collection(CollectionC)

	AIds := []primitive.ObjectID{
		primitive.NewObjectID(),
		primitive.NewObjectID(),
		primitive.NewObjectID(),
	}
	BIds := []primitive.ObjectID{
		primitive.NewObjectID(),
		primitive.NewObjectID(),
		primitive.NewObjectID(),
		primitive.NewObjectID(),
		primitive.NewObjectID(),
		primitive.NewObjectID(),
		primitive.NewObjectID(),
		primitive.NewObjectID(),
		primitive.NewObjectID(),
		primitive.NewObjectID(),
		primitive.NewObjectID(),
		primitive.NewObjectID(),
		primitive.NewObjectID(),
		primitive.NewObjectID(),
	}
	CIds := []primitive.ObjectID{
		primitive.NewObjectID(),
		primitive.NewObjectID(),
		primitive.NewObjectID(),
	}

	As := []interface{}{
		bson.D{{Key: "_id", Value: AIds[0]}, {Key: "aaaa", Value: 123213}, {Key: "333", Value: "OKOK Aa"}},
		bson.D{{Key: "_id", Value: AIds[1]}, {Key: "aaa2", Value: 1232}, {Key: "33", Value: "OKOK AaAa"}},
		bson.D{{Key: "_id", Value: AIds[2]}, {Key: "aaa3", Value: 1232}, {Key: "33a3", Value: "OKOK AaAa3"}},
	}

	Bs := []interface{}{
		bson.D{{Key: "_id", Value: BIds[0]}, {Key: "aId", Value: AIds[0]}},
		bson.D{{Key: "_id", Value: BIds[1]}, {Key: "aId", Value: AIds[0]}},
		bson.D{{Key: "_id", Value: BIds[2]}, {Key: "aId", Value: AIds[0]}},
		bson.D{{Key: "_id", Value: BIds[3]}, {Key: "aId", Value: AIds[0]}},
		bson.D{{Key: "_id", Value: BIds[4]}, {Key: "aId", Value: AIds[1]}},
		bson.D{{Key: "_id", Value: BIds[5]}, {Key: "aId", Value: AIds[1]}},
		bson.D{{Key: "_id", Value: BIds[6]}, {Key: "aId", Value: AIds[1]}},
		bson.D{{Key: "_id", Value: BIds[7]}, {Key: "aId", Value: AIds[1]}},
		bson.D{{Key: "_id", Value: BIds[8]}, {Key: "aId", Value: AIds[1]}},
		bson.D{{Key: "_id", Value: BIds[9]}, {Key: "aId", Value: AIds[2]}},
		bson.D{{Key: "_id", Value: BIds[10]}, {Key: "aId", Value: AIds[2]}},
		bson.D{{Key: "_id", Value: BIds[11]}, {Key: "aId", Value: AIds[2]}},
		bson.D{{Key: "_id", Value: BIds[12]}, {Key: "aId", Value: AIds[2]}},
		bson.D{{Key: "_id", Value: BIds[13]}, {Key: "aId", Value: AIds[2]}},
	}

	Cs := []interface{}{
		bson.D{{Key: "_id", Value: CIds[0]}, {Key: "aId", Value: AIds[0]}, {Key: "bIds", Value: primitive.A{
			BIds[0],
			BIds[1],
			BIds[2],
			BIds[3],
		}}},
		bson.D{{Key: "_id", Value: CIds[1]}, {Key: "aId", Value: AIds[1]}, {Key: "bIds", Value: primitive.A{
			BIds[4],
			BIds[5],
			BIds[6],
			BIds[7],
			BIds[8],
		}}},
		bson.D{{Key: "_id", Value: CIds[2]}, {Key: "aId", Value: AIds[2]}, {Key: "bIds", Value: primitive.A{
			BIds[9],
			BIds[10],
			BIds[11],
			BIds[12],
			BIds[13],
		}}},
	}

	if _, err := A.InsertMany(ctx, As); err != nil {
		log.Fatalf("Error during inserting: %s", err)
	}
	if _, err := B.InsertMany(ctx, Bs); err != nil {
		log.Fatalf("Error during inserting: %s", err)
	}
	if _, err := C.InsertMany(ctx, Cs); err != nil {
		log.Fatalf("Error during inserting: %s", err)
	}

	fmt.Println("Database filled")
}
