package discover

import (
	"context"
	"fmt"
	"log"
	"sync"

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

type cacheExists struct {
	*sync.RWMutex
	m map[string]bool
}

type cacheCollection struct {
	*sync.RWMutex
	m map[string][]string
}

// Discover will walk trought Database using its repo and collect some data about the schema
type Discover struct {
	cacheExists     cacheExists
	cacheCollection cacheCollection
	repo            Repo
}

// New returns a new discover
func New(r Repo) *Discover {
	return &Discover{
		repo:            r,
		cacheExists:     cacheExists{m: make(map[string]bool), RWMutex: &sync.RWMutex{}},
		cacheCollection: cacheCollection{m: make(map[string][]string), RWMutex: &sync.RWMutex{}},
	}
}

// Link represents a path that leads to an ObjectId as a string
type Link struct {
	Value   string `json:"-"`
	Path    string
	With    []string
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
		} else if a, ok := v.(primitive.A); ok {
			for _, el := range a {
				if e, ok := el.(primitive.M); ok {
					subls, err := Linkify(e, fmt.Sprintf("%s%s.$", path, p))
					if err != nil {
						return ls, err
					}

					ls = append(ls, subls...)
				}
			}
		}
	}

	return ls, nil
}

// matchLink tries to match Links against all collection to find
// if n is the ls's length d.matchLink will return n Link if all ids are found
func (d Discover) matchLink(ctx context.Context, ls []Link) ([]Link, error) {
	matchLs := []Link{}

	dbs, err := d.repo.ListDatabases(ctx)
	if err != nil {
		return nil, fmt.Errorf("Error during fetching Db names: %w", err)
	}

	for _, l := range ls {
		withCancel, cancel := context.WithCancel(ctx)
		defer cancel()
		ch := make(chan string, 1) //expecting that only 1 db.collection will match id
		defer close(ch)
		wg := sync.WaitGroup{}
		for _, dbase := range dbs {
			db := dbase
			if db == "config" || db == "system" || db == "admin" || db == "local" {
				continue
			}
			cls, err := d.listCollectionsWithCache(ctx, db)
			if err != nil {
				return nil, fmt.Errorf("Error during fetching collection names for db: %s with: %w", db, err)
			}

			for _, c := range cls {
				cl := c
				wg.Add(1)

				go func() {
					defer wg.Done()
					exists, err := d.existsByIDWithCache(withCancel, db, cl, l.Value)
					if err != nil {
						log.Printf("Error during existingID: %v\n", err)
					}

					if exists {
						ch <- fmt.Sprintf("%s.%s", db, cl)
						cancel()
					}
				}()
			}
		}

		wg.Wait()

		select {
		case p := <-ch:
			nl := l
			nl.With = append(nl.With, p)
			matchLs = append(matchLs, nl)
		default:
			log.Printf("Unknow OID: %s\n", l.Value)
		}
	}

	return matchLs, nil
}

// reduceLinks will compute all probabilities that a specific link exists
func reduceLinks(lss [][]Link) (CollectionLinks, error) {
	m := make(map[string]struct {
		n    int
		with []string
	})

	for _, ls := range lss {
		for _, l := range ls {
			c := m[l.Path]

			if !contains(c.with, l.With[0]) {
				c.with = append(c.with, l.With...)
			}

			c.n = c.n + 1

			m[l.Path] = c
		}
	}

	mL := make(CollectionLinks)
	for p, c := range m {
		mL[p] = Link{
			Path:    p,
			Percent: float32(c.n) / float32(len(lss)),
			With:    c.with,
		}
	}

	return mL, nil
}

func contains(ss []string, match string) bool {
	for _, s := range ss {
		if s == match {
			return true
		}
	}
	return false
}

func (d Discover) existsByIDWithCache(ctx context.Context, db string, c string, idStr string) (bool, error) {
	path := fmt.Sprintf("%s.%s:%s", db, c, idStr)
	d.cacheExists.RLock()
	b, ok := d.cacheExists.m[path]
	d.cacheExists.RUnlock()

	if ok {
		return b, nil
	}

	id, err := primitive.ObjectIDFromHex(idStr)
	if err != nil {
		return false, fmt.Errorf("Error during ObjectId creation with value: %s, with: %s", id, err)
	}

	exists, err := d.repo.ExistsByID(ctx, db, c, id)
	if err != nil {
		return false, fmt.Errorf("Error during searching %s in %s.%s with: %w", id.Hex(), db, c, err)
	}

	d.cacheExists.Lock()
	d.cacheExists.m[path] = exists
	d.cacheExists.Unlock()

	return exists, err
}

func (d Discover) listCollectionsWithCache(ctx context.Context, db string) ([]string, error) {
	path := db
	d.cacheCollection.RLock()
	l, ok := d.cacheCollection.m[path]
	d.cacheCollection.RUnlock()
	if ok {
		return l, nil
	}

	cls, err := d.repo.ListCollections(ctx, db)
	if err != nil {
		return []string{}, fmt.Errorf("Error during listing collection for %s with: %w", db, err)
	}

	d.cacheCollection.Lock()
	d.cacheCollection.m[path] = cls
	d.cacheCollection.Unlock()

	return cls, err
}

// Collection retrieves all path that can be an ObjectId
func (d Discover) Collection(ctx context.Context, db string, collection string) (CollectionLinks, error) {
	samples, err := d.repo.SampleCollection(ctx, db, collection, sampleSize)
	if err != nil {
		log.Printf("Error during fetching sample of collection: %s db: %s with err: %s", collection, db, err)
		return nil, fmt.Errorf("Error during fetching sample of collection: %s db: %s with err: %s", collection, db, err)
	}

	lss := make([][]Link, 0, len(samples))

	for _, m := range samples {
		ls, err := Linkify(m, "")
		if err != nil {
			log.Printf("Error during Linkify %s", err)
			return nil, fmt.Errorf("Error during Linkify %s", err)
		}

		ls, err = d.matchLink(ctx, ls)
		lss = append(lss, ls)
		if err != nil {
			return nil, fmt.Errorf("Error during MatchLink for %s.%s with: %w", db, collection, err)
		}
	}

	return reduceLinks(lss)
}

// Database returns all links about all collections inside a Database
func (d Discover) Database(ctx context.Context, db string) (map[string]CollectionLinks, error) {
	cls, err := d.repo.ListCollections(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("Error during ListCollections(): %w", err)
	}

	log.Printf("Found %d collections for %s\n", len(cls), db)
	mCls := map[string]CollectionLinks{}
	wg := sync.WaitGroup{}
	ch := make(chan work)
	errCh := make(chan error)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, cl := range cls {
		c := cl
		go func() {
			wg.Add(1)
			cm, errC := d.Collection(ctx, db, c)
			if errC != nil {
				errCh <- errC
			}
			ch <- work{path: c, c: cm}

			wg.Done()
		}()
	}
	go func() {
		for {
			select {
			case c := <-ch:
				log.Printf("%s.%s done!\n", db, c.path)
				mCls[c.path] = c.c
			case err := <-errCh:
				log.Printf("Error during scanning collection: %v", err)
				cancel()
			case <-ctx.Done():
				return
			}
		}
	}()
	wg.Wait()

	log.Printf("%d ObjectId scanned !\n", len(d.cacheExists.m))
	return mCls, nil
}

type work struct {
	c    CollectionLinks
	path string
}
