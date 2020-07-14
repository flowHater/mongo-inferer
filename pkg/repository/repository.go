package repository

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// Repo contains all methods to access to Mongodb
type Repo struct {
	client *mongo.Client
}

// OptionF describes a func that will be called from the New func
type OptionF func(*Repo)

// WithClient allows caller to set a specific client which will be used to request
func WithClient(c *mongo.Client) OptionF {
	return func(r *Repo) {
		r.client = c
	}
}

// New creates a new repository
func New(opts ...OptionF) *Repo {
	r := &Repo{}

	for _, o := range opts {
		o(r)
	}

	return r
}

// ExistsByID tests the existence of a document by its ID in a specific database collection
func (r Repo) ExistsByID(ctx context.Context, db, collection string, id primitive.ObjectID) (bool, error) {
	c, err := r.client.Database(db).Collection(collection).Find(ctx, primitive.M{"_id": id})
	if err != nil {
		return false, fmt.Errorf("Error during fetching %s in %s.%s with: %w", id, db, collection, err)
	}

	return c.Next(ctx), nil
}

// ListDatabases will return all database names that client can access
func (r Repo) ListDatabases(ctx context.Context) ([]string, error) {
	return r.client.ListDatabaseNames(ctx, primitive.M{})
}

// ListCollections will return all collection names for a specific db
func (r Repo) ListCollections(ctx context.Context, db string) ([]string, error) {
	return r.client.Database(db).ListCollectionNames(ctx, primitive.M{})
}

// SampleCollection returns a random sample of a specific size from a specific db.collection
func (r Repo) SampleCollection(ctx context.Context, db, collection string, size int) (*mongo.Cursor, error) {
	return r.client.Database(db).Collection(collection).Aggregate(ctx, primitive.A{
		primitive.D{{Key: "$sample", Value: primitive.D{{Key: "size", Value: size}}}},
	})
}
