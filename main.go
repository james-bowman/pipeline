package main

import (
	"log"
	"time"
	"os"
	"github.com/james-bowman/pipeline/datasrc"
)

func main() {
	storeUrl := os.Getenv("STORE_ADDR")
	feedUrl := os.Getenv("FEED_ADDR")

	if storeUrl == "" {
		storeUrl = "http://localhost:8001"
	}
	if feedUrl == "" {
		feedUrl = "http://localhost:8000"
	}

	log.Printf("Using Feed at %s\n", feedUrl)
	log.Printf("Using Store at %s\n", storeUrl)

	events, err := datasrc.NewEventRepository(feedUrl)
	if err != nil {
		log.Fatalf("Failed to create EventRepository because %v", err)
	}

	markets, err := datasrc.NewMarketRepository(feedUrl)
	if err != nil {
		log.Fatalf("Failed to create MarketRepository because %v", err)
	}

	feed := NewFeed(events, time.Duration(5*time.Second))
	stream := NewEventStream(feed.NewItems(), events, markets)

	store, err := NewEventStore(storeUrl)
	if err != nil {
		log.Fatalf("Failed to create Store because %v", err)
	}

	sink := NewEventSink(stream.Events(), store)
	sink.ProcessAll()
}
