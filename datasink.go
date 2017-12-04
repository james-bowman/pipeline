package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"
)

const (
	uriPath = "/event"
)

type Event struct {
	Id      string    `json:"id"`
	Name    string    `json:"name"`
	Time    time.Time `json:"time"`
	Markets []Market  `json:"markets"`
}

type Market struct {
	Id      string   `json:"id"`
	Type    string   `json:"type"`
	Options []Option `json:"options"`
}

type Option struct {
	Id   string `json:"id"`
	Name string `json:"name"`
	Num  int    `json:"num"`
	Den  int    `json:"den"`
}

type EventStore struct {
	BaseURL *url.URL
}

func NewEventStore(baseURL string) (*EventStore, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	u.Path = "/event"
	return &EventStore{BaseURL: u}, nil
}

func (e *EventStore) Create(event *Event) error {
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(event)

	resp, err := http.Post(e.BaseURL.String(), "application/json; charset=utf-8", b)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Event Store rejected Event %v because %s", event, resp.Status)
	}

	return nil
}

type EventSink struct {
	eventStore  *EventStore
	events      <-chan Event
	termination chan struct{}
}

func NewEventSink(events <-chan Event, eventStore *EventStore) *EventSink {
	var e EventSink

	e = EventSink{
		eventStore: eventStore,
		events:     events,
	}

	return &e
}

func (e *EventSink) ProcessAll() {
	for event := range e.events {
		if err := e.eventStore.Create(&event); err != nil {
			log.Printf("Failed to store Event %v because %v\n", event, err)
			continue
		}
	}
}
