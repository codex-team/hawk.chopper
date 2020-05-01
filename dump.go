package main

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"log"
	"os"
	"sort"
	"strings"
	"time"
)

var db *mongo.Database
var mostRecentCollectionsLimit = 10
var dailyEventsLimit int64 = 10
var repetitionsLimit int64 = 10

type Empty struct {}

type StringSet map[string]Empty

type DailyEvent struct {
	LastRepetitionTime int32 `bson:"lastRepetitionTime"`
	GroupHash string `bson:"groupHash"`
}

type AggregatedResult struct {
	Id primitive.ObjectID `bson:"_id"`
	GroupHash string `bson:"groupHash"`
	Count int32 `bson:"count"`
	GroupingTimestamp int32 `bson:"groupingTimestamp"`
	LastRepetitionTime float64 `bson:"lastRepetitionTime"`
	Events []bson.Raw `bson:"events"`
	Repetitions []bson.Raw `bson:"repetitions"`
}

func getCollectionNames() []string {
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	collections, err := db.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		log.Fatalf("Error connect: %s", err)
	}

	var filteredCollections []string
	for _, collection := range collections {
		if strings.HasPrefix(collection, "dailyEvents") {
			filteredCollections = append(filteredCollections, collection)
		}
	}

	return filteredCollections
}

func getMostNewCollections() []string {
	type CollectionDate struct {
		Id string
		LastRepetitionTime int32
	}

	dailyEventsCollections := getCollectionNames()

	var lastRepetitionTimes = make([]CollectionDate, len(dailyEventsCollections))
	for i := range dailyEventsCollections {
		ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)

		var event DailyEvent
		db.Collection(dailyEventsCollections[i]).FindOne(ctx, bson.M{}, &options.FindOneOptions{Sort:bson.M{"lastRepetitionTime": -1}}).Decode(&event)
		lastRepetitionTimes[i] = CollectionDate{dailyEventsCollections[i], event.LastRepetitionTime}
	}

	sort.Slice(lastRepetitionTimes[:], func(i, j int) bool {
		return lastRepetitionTimes[i].LastRepetitionTime > lastRepetitionTimes[j].LastRepetitionTime
	})

	var mostRecentCollections []string
	for i, collection := range lastRepetitionTimes {
		if i >= mostRecentCollectionsLimit {
			break
		}
		mostRecentCollections = append(mostRecentCollections, collection.Id)
	}

	return mostRecentCollections
}

func getLastDailyEvents(collection string) (StringSet) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	cur, err := db.Collection(collection).Find(ctx, bson.M{}, &options.FindOptions{
		Sort:bson.M{"lastRepetitionTime": -1},
		Limit:&dailyEventsLimit,
	})
	if err != nil {
		log.Fatalf("getLastDailyEvents error: %s", err)
	}

	writer, err := os.Create(fmt.Sprintf("./dump/%s.bson", collection))
	if err != nil {
		log.Fatalf("File open error: %s", err)
	}
	defer writer.Close()

	var groupHashes = make(StringSet)
	for cur.Next(ctx) {
		var result bson.Raw
		err := cur.Decode(&result)
		if err != nil { log.Fatal(err) }

		var event DailyEvent
		err = cur.Decode(&event)
		if err != nil {
			log.Printf("%s: %s", collection, err)
			continue
		}

		if _, present := groupHashes[event.GroupHash]; !present {
			groupHashes[event.GroupHash] = struct{}{}
		}

		_, err = writer.Write(cur.Current)
		if err != nil {
			log.Fatalf("error writing to file: %v", err)
		}
	}
	if err := cur.Err(); err != nil {
		log.Fatalf("getLastDailyEvents %s: %s", collection, err)
	}

	return groupHashes
}

func getEvents(collectionId string) []AggregatedResult {
	ctx, _ := context.WithTimeout(context.Background(), 180*time.Second)

	limitDailyEventsStage := bson.D{{"$limit", dailyEventsLimit}}
	limitRepetitionsStage := bson.D{{"$limit", repetitionsLimit}}

	lookupErrorStage := bson.D{{"$lookup", bson.D{
		{"from", fmt.Sprintf("events:%s", collectionId)},
		{"localField", "groupHash"},
		{"foreignField", "groupHash"},
		{"as", "events"},
	}}}
	lookupRepetitionsStage := bson.D{{"$lookup", bson.D{
		{"from", fmt.Sprintf("repetitions:%s", collectionId)},
		{"as", "repetitions"},
		{"pipeline", mongo.Pipeline{
			bson.D{{"$match", bson.D{
				{"$expr", bson.M{"$eq": bson.A{"$groupHash", "$groupHash"}}},
			}}},
			limitRepetitionsStage,
		}},
	}}}

	cur, err := db.Collection(fmt.Sprintf("dailyEvents:%s", collectionId)).Aggregate(ctx, mongo.Pipeline{
		limitDailyEventsStage,
		lookupErrorStage,
		lookupRepetitionsStage,
	})

	if err != nil {
		log.Fatalf("getLastDailyEvents error: %s", err)
	}

	var lastEvents []AggregatedResult
	for cur.Next(ctx) {
		var result AggregatedResult
		err := cur.Decode(&result)
		if err != nil { log.Fatal(err) }
		lastEvents = append(lastEvents, result)

		//_, err = writer.Write(cur.Current)
		//if err != nil {
		//	log.Fatalf("error writing to file: %v", err)
		//}

	}
	if err := cur.Err(); err != nil {
		log.Fatal(err)
	}

	return lastEvents
}


func main() {
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://127.0.0.1:27018/?connect=direct"))
	if err != nil {
		log.Fatalf("Error client creation: %s", err)
	}

	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		log.Fatalf("Error connect: %s", err)
	}

	ctx, _ = context.WithTimeout(context.Background(), 3*time.Second)
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		log.Fatalf("Error ping: %s", err)
	}

	db = client.Database("hawk_events")

	collections := getMostNewCollections()
	for _, collectionName := range collections {
		log.Printf("Start processing %s\n", collectionName)
		parts := strings.SplitN(collectionName, ":", 2)
		//events := getEvents(parts[1])

		groupHashes := getLastDailyEvents(collectionName)
		getEventsByDailyEvents(fmt.Sprintf("events:%s", parts[1]), groupHashes)
		saveRepetitionsByGroupHashes(fmt.Sprintf("repetitions:%s", parts[1]), groupHashes)


	//
	//	var bs bson.M
	//	bs = bson.M{"dump": events}
	//	bt, err := bson.Marshal(&bs)
	//	if err != nil {
	//		log.Fatalf("Marshal error: %s", err)
	//	}
	//
	//	ioutil.WriteFile(filepath.Join("dump", fmt.Sprintf("%s.bson", parts[1])), bt, os.ModePerm)
	}


	log.Printf("Done")
}
