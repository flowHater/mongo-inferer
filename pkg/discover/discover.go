package discover

import (
	"context"
	"fmt"
	"log"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	sampleSize = 100
	primaryKey = "_id"
)

// Repo describes all methods needed by Discover
type Repo interface {
	ExistsByID(ctx context.Context, db, collection string, id primitive.ObjectID) (bool, error)
	ListDatabases(ctx context.Context) ([]string, error)
	ListCollections(ctx context.Context, db string) ([]string, error)
	SampleCollection(ctx context.Context, db, collection string, size int) ([]primitive.M, error)
}

// Discover will walk trought Database using its repo and collect some data about the schema
type Discover struct {
	repo Repo
}

// New returns a new discover
func New(r Repo) *Discover {
	return &Discover{
		repo: r,
	}
}

// Link represents a path that leads to an ObjectId as a string
type Link struct {
	Value   string
	Path    string
	With    string
	Percent float32
}

// CollectionLinks is a map with all Links found in the database
type CollectionLinks map[string]Link

// Linkify transforms an primitive.M to a slice of Link
func Linkify(m primitive.M, currentPath string) ([]Link, error) {
	ls := []Link{}
	var path string
	if currentPath != "" {
		path = currentPath + "."
	}

	for p, v := range m {
		if p == primaryKey {
			continue
		}

		if id, ok := v.(primitive.ObjectID); ok {
			l := Link{
				Path:  path + p,
				Value: id.Hex(),
			}
			ls = append(ls, l)
		} else if m, ok := v.(primitive.M); ok {
			subls, err := Linkify(m, path+p)
			if err != nil {
				return ls, err
			}

			ls = append(ls, subls...)
		} else if s, ok := v.(string); ok {
			if _, err := primitive.ObjectIDFromHex(s); err == nil {
				l := Link{
					Value: s,
					Path:  path + p,
				}

				ls = append(ls, l)
			}
		}
	}

	return ls, nil
}

// matchLink tries to match Links against all collection to find
func (d Discover) matchLink(ctx context.Context, ls []Link) ([]Link, error) {
	matchLs := []Link{}

	dbs, err := d.repo.ListDatabases(ctx)
	if err != nil {
		log.Fatalf("Error during fetching Db names: %s", err)
	}

	for _, db := range dbs {
		if db == "config" || db == "system" || db == "admin" || db == "local" {
			continue
		}

		cls, err := d.repo.ListCollections(ctx, db)
		if err != nil {
			log.Fatalf("Error during fetching collection names for db: %s with: %s", db, err)
		}
		for _, c := range cls {
			for _, l := range ls {
				id, err := primitive.ObjectIDFromHex(l.Value)
				if err != nil {
					log.Fatalf("Error during ObjectId creation with value: %s, with: %s", l.Value, err)
				}

				exists, err := d.repo.ExistsByID(ctx, db, c, id)
				if err != nil {
					log.Fatalf("Error during searching %s in %s.%s with: %s", id.Hex(), db, c, err)
				}

				if exists {
					nl := l
					nl.With = fmt.Sprintf("%s.%s", db, c)
					matchLs = append(matchLs, nl)
				}
			}
		}
	}

	return matchLs, nil
}

// reduceLinks will compute all probabilities that a specific link exists
func reduceLinks(lss [][]Link) (CollectionLinks, error) {
	m := make(map[string]struct {
		n    int
		with string
	})
	mL := make(CollectionLinks)

	for _, ls := range lss {
		for _, l := range ls {
			c := m[l.Path]

			c.n = c.n + 1
			c.with = l.With
			m[l.Path] = c
		}
	}

	for p, c := range m {
		mL[p] = Link{
			Path:    p,
			Percent: float32(c.n) / float32(sampleSize),
			With:    c.with,
		}
	}

	return mL, nil
}

// Collection allow to retrieve all path that can be an ObjectId
func (d Discover) Collection(ctx context.Context, db string, collection string) (CollectionLinks, error) {

	samples, err := d.repo.SampleCollection(ctx, db, collection, sampleSize)
	if err != nil {
		log.Fatalf("Error during fetching sample of collection: %s db: %s with err: %s", collection, db, err)
	}

	lss := make([][]Link, 0, len(samples))

	for _, m := range samples {
		ls, err := Linkify(m, "")
		if err != nil {
			log.Fatalf("Error during Linkify %s", err)
		}

		ls, err = d.matchLink(ctx, ls)
		lss = append(lss, ls)
		if err != nil {
			log.Fatalf("Error during MatchLink for %s.%s with: %s", db, collection, err)
		}
	}

	return reduceLinks(lss)
}
