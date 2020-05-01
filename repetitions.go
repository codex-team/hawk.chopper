package main

import (
	"context"
	"fmt"
	"github.com/cheggaaa/pb/v3"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"time"
)

func saveRepetitionsByGroupHashes(collection string, groupHashes StringSet) {
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)

	err := saveIndexesToFile(collection)
	if err != nil {
		log.Printf("saveIndexesToFile error: %s", err)
	}

	writer := createWriterToFile(fmt.Sprintf("%s.bson", collection))
	defer writer.Close()

	bar := pb.StartNew(len(groupHashes))
	for groupHash := range groupHashes {
		cur, err := db.Collection(collection).Find(ctx, bson.M{
			"groupHash": groupHash,
		}, &options.FindOptions{
			Limit:&cfg.MaxRepetitions,
		})
		if err != nil {
			log.Fatalf("error get: %s", err)
		}
		for cur.Next(ctx) {
			_, err = writer.Write(cur.Current)
			if err != nil {
				log.Fatalf("error writing to file: %v", err)
			}
		}
		bar.Increment()
	}
	bar.Finish()
}
