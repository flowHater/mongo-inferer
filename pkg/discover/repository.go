package discover

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo/options"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// Repository contains all methods to access to Mongodb
type Repository struct {
	client *mongo.Client
}

// RepositoryOptionF describes a func that will be called from the New func
type RepositoryOptionF func(*Repository)

// RepositoryWithClient allows caller to set a specific client which will be used to request
func RepositoryWithClient(c *mongo.Client) RepositoryOptionF {
	return func(r *Repository) {
		r.client = c
	}
}

// NewRepository creates a new Repository
func NewRepository(opts ...RepositoryOptionF) *Repository {
	r := &Repository{}

	for _, o := range opts {
		o(r)
	}

	return r
}

// ExistsByID tests the existence of a document by its ID in a specific database collection
func (r Repository) ExistsByID(ctx context.Context, db, collection string, id primitive.ObjectID) (bool, error) {
	c, err := r.client.Database(db).Collection(collection).Find(ctx,
		primitive.M{"_id": id},
		options.Find().SetProjection(primitive.M{"_id": 1}).SetLimit(1),
	)

	if err != nil {
		if ctx.Err() == context.Canceled {
			// The context can be canceled if another goroutine found a matching collection before this one
			return false, nil
		}

		return false, fmt.Errorf("Error during fetching %s in %s.%s with: %w", id, db, collection, err)
	}

	return c.Next(ctx), nil
}

// ListDatabases will return all database names that client can access
func (r Repository) ListDatabases(ctx context.Context) ([]string, error) {
	return r.client.ListDatabaseNames(ctx, primitive.M{})
}

// ListCollections will return all collection names for a specific db
func (r Repository) ListCollections(ctx context.Context, db string) ([]string, error) {
	return r.client.Database(db).ListCollectionNames(ctx, primitive.M{})
}

// SampleCollection returns a random sample of a specific size from a specific db.collection
func (r Repository) SampleCollection(ctx context.Context, db, collection string, size int) ([]primitive.M, error) {
	c, err := r.client.Database(db).Collection(collection).Aggregate(ctx, primitive.A{
		primitive.D{{Key: "$sample", Value: primitive.D{{Key: "size", Value: size}}}},
	}, options.Aggregate().SetAllowDiskUse(true))

	if err != nil {
		return nil, fmt.Errorf("Error during sampling: %w", err)
	}

	results := []primitive.M{}
	err = c.All(ctx, &results)

	return results, err
}
