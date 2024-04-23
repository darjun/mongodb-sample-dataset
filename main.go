package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found")
	}

	uri := os.Getenv("MONGODB_URI")
	if uri == "" {
		log.Fatal("You must set your 'MONGODB_URI' environment variable.")
	}

	countPerBathStr := os.Getenv("COUNT_PER_BATCH")
	if countPerBathStr == "" {
		countPerBathStr = "1000"
	}
	countPerBath, err := strconv.ParseInt(countPerBathStr, 10, 64)
	if err != nil {
		panic(err)
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
				decoder := json.NewDecoder(bytes.NewReader(data))
				for {
					var document interface{}
					err = decoder.Decode(&document)
					if err != nil {
						fmt.Printf("decode error:%v\n", err)
						break
					}
					documents = append(documents, document)
				}

				coll := client.Database(entry.Name()).Collection(strings.TrimSuffix(file.Name(), ".json"))
				for page := 0; page <= (len(documents)-1)/int(countPerBath); page++ {
					begin := page * int(countPerBath)
					end := (page + 1) * int(countPerBath)
					if end > len(documents) {
						end = len(documents)
					}
					result, err := coll.InsertMany(context.TODO(), documents[begin:end])
					if err != nil {
						fmt.Printf("%#v\n", result)
					}
				}
			}
		}
	}
}
