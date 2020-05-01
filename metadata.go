package main

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"log"
	"time"
)

type Metadata struct {
	Options        bson.M   `bson:"options,omitempty"`
	Indexes        []bson.D `bson:"indexes"`
	UUID           string   `bson:"uuid,omitempty"`
}


func saveIndexesToFile(collection string) error {
	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
	indexesIter, err := db.Collection(collection).Indexes().List(ctx)
	if err != nil {
		return err
	}
	defer indexesIter.Close(context.Background())

	meta := Metadata{
		Indexes: []bson.D{},
		Options: bson.M{},
	}

	for indexesIter.Next(ctx) {
		indexOpts := &bson.D{}
		err := indexesIter.Decode(indexOpts)
		if err != nil {
			log.Fatalf("error converting index: %v", err)
		}
		meta.Indexes = append(meta.Indexes, *indexOpts)
	}

	writer := createWriterToFile(fmt.Sprintf("%s.metadata.json", collection))
	defer writer.Close()

	jsonBytes, err := bson.MarshalExtJSON(meta, true, false)
	if err != nil {
		return fmt.Errorf("error marshalling metadata json for collection `%v`: %v", collection, err)
	}

	_, err = writer.Write(jsonBytes)
	if err != nil {
		log.Fatalf("Write metadata error: %s", err)
	}

	return nil
}
