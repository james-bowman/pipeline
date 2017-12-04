package main

import (
	"fmt"
	"github.com/james-bowman/pipeline/datasrc"
	"log"
	"strconv"
	"strings"
	"time"
)

type NewItemChecker interface {
	CheckForNewItems() ([]int, error)
}

// Feed represents a connection to a published data feed.  In this case, the
// feed exposes a URL that publishes the IDs of (new?) events that may in turn
// be retrieved
type Feed struct {
	checker     NewItemChecker
	Frequency   time.Duration
	newItems    chan int
	termination chan struct{}
}

func NewFeed(checker NewItemChecker, pollFrequency time.Duration) *Feed {
	var feed Feed

	feed = Feed{
		checker:     checker,
		Frequency:   pollFrequency,
		newItems:    make(chan int),
		termination: make(chan struct{}),
	}

	go feed.process()

	return &feed
}

func (f Feed) Close() {
	close(f.termination)
}

func (f Feed) NewItems() <-chan int {
	return f.newItems
}

// loop to periodically check for new events in the feed and push them as
// updates
func (f Feed) process() {
	var items []int
	var fetchDelay time.Duration

	for {

		timeToCheck := time.After(fetchDelay)

		select {
		// check for new events
		case <-timeToCheck:
			var err error
			items, err = f.checker.CheckForNewItems()
			if err != nil {
				log.Printf("Failed to check for new Event IDs because %v", err)
				break
			}

		// shutdown
		case <-f.termination:
			close(f.newItems)
			return
		}

		fetchDelay = f.Frequency

		for _, item := range items {
			select {
			// send next new item
			case f.newItems <- item:

			// shutdown
			case <-f.termination:
				close(f.newItems)
				return
			}
		}
		items = nil
	}
}

type EventSourcer interface {
	GetByID(int) (*datasrc.Event, error)
}

type MarketSourcer interface {
	GetByID(int) (*datasrc.Market, error)
}

type EventStream struct {
	eventSrc    EventSourcer
	marketSrc   MarketSourcer
	newIds      <-chan int
	events      chan Event
	termination chan struct{}
}

func NewEventStream(newIds <-chan int, eventSrc EventSourcer, marketSrc MarketSourcer) *EventStream {
	var e EventStream

	e = EventStream{
		eventSrc:    eventSrc,
		marketSrc:   marketSrc,
		newIds:      newIds,
		events:      make(chan Event),
		termination: make(chan struct{}),
	}

	e.process()

	return &e
}

func (e *EventStream) Events() <-chan Event {
	return e.events
}

func (e *EventStream) Close() {
	close(e.termination)
}

func (e *EventStream) process() {
	events := make(chan datasrc.Event)
	go func() {
		defer close(events)
		for id := range e.newIds {
			newEvent, err := e.eventSrc.GetByID(id)
			if err != nil {
				log.Printf("Failed to source Event for Id %d because %v\n", id, err)
				continue
			}

			select {
			case events <- *newEvent:
			case <-e.termination:
				return
			}
		}
	}()

	go func() {
		defer close(e.events)
		for event := range events {

			product, err := e.convertEvent(&event)
			if err != nil {
				log.Printf("Failed to convert Event %#v because %v\n", event, err)
				continue
			}

			select {
			case e.events <- *product:
			case <-e.termination:
				return
			}
		}
	}()
}

func (e *EventStream) convertEvent(event *datasrc.Event) (*Event, error) {
	var markets []Market
	for _, marketId := range event.Markets {
		m, err := e.marketSrc.GetByID(marketId)
		if err != nil {
			return nil, err
		}

		var options []Option
		for _, o := range m.Options {
			var num, den int
			odds := strings.Split(o.Odds, "/")
			if len(odds) < 2 {
				return nil, fmt.Errorf("Failed to parse odds %s for market %d", o.Odds, marketId)
			}
			if num, err = strconv.Atoi(odds[0]); err != nil {
				return nil, fmt.Errorf("Failed to parse odds %s for market %d", o.Odds, marketId)
			}
			if den, err = strconv.Atoi(odds[1]); err != nil {
				return nil, fmt.Errorf("Failed to parse odds %s for market %d", o.Odds, marketId)
			}
			option := Option{
				Id:   o.Id,
				Name: o.Name,
				Num:  num,
				Den:  den,
			}
			options = append(options, option)
		}

		market := Market{
			Id:      m.Id,
			Type:    m.Type,
			Options: options,
		}
		markets = append(markets, market)
	}

	product := Event{
		Id:      strconv.Itoa(event.Id),
		Name:    event.Name,
		Time:    event.Time,
		Markets: markets,
	}
	return &product, nil
}
