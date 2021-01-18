package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"

	"github.com/flowHater/mongo-inferer/pkg/discover"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type arrayFlags []string

func (i *arrayFlags) String() string {
	return fmt.Sprintf("%v", *i)
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

var myFlags arrayFlags

func main() {
	dbs := &arrayFlags{}
	allowFullScan := false
	flag.Var(dbs, "db", "database to infer")
	flag.BoolVar(&allowFullScan, "fullScan", false, "should infer all kind of field (Not only Link)")
	uri := flag.String("uri", "mongodb://localhost:27017", "uri used for connection to mongodb")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(*uri))
	defer client.Disconnect(ctx)
	if err != nil {
		log.Fatalf("An error occured during mongodb client's initialization")
	}

	r := discover.NewRepository(discover.RepositoryWithClient(client))
	d := discover.New(ctx, r, discover.AllowFullScan(allowFullScan))
	m := make(map[string]map[string]discover.CollectionLinks)
	for _, db := range *dbs {
		md, err := d.Database(ctx, db)
		if err != nil {
			log.Println(err)
		}
		m[db] = md
	}

	jm, err := json.Marshal(m)
	if err != nil {
		log.Fatalln("Error during Marschalling json: ", err)
	}

	fmt.Println(string(jm))
}
