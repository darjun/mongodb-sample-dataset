package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"os"
	"strings"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found")
	}

	uri := os.Getenv("MONGODB_URI")
	if uri == "" {
		log.Fatal("You must set your 'MONGODB_URI' environment variable.")
	}

	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(uri))
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := client.Disconnect(context.TODO()); err != nil {
			panic(err)
		}
	}()

	entries, err := os.ReadDir("./")
	if err != nil {
		panic(err)
	}

	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "sample_") {
			files, err := os.ReadDir("./" + entry.Name())
			if err != nil {
				panic(err)
			}

			for _, file := range files {
				if !strings.HasSuffix(file.Name(), ".json") {
					continue
				}

				data, err := os.ReadFile("./" + entry.Name() + "/" + file.Name())
				if err != nil {
					panic(err)
				}

				var documents []interface{}
				err = json.Unmarshal(data, &documents)
				if err != nil {
					panic(err)
				}

				coll := client.Database(entry.Name()).Collection(strings.TrimSuffix(file.Name(), ".json"))
				result, err := coll.InsertMany(context.TODO(), documents)
				if err != nil {
					fmt.Printf("%#v\n", result)
				}
			}
		}
	}
}
