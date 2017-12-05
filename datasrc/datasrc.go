package datasrc

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

const (
	uriPath = "/football/events"
)

type Event struct {
	Id      int
	Name    string
	Time    time.Time
	Markets []int
}

func (e *Event) UnmarshalJSON(data []byte) error {
	var err error
	alias := &struct {
		Id      int    `json:"id"`
		Name    string `json:"name"`
		Time    string `json:"time"`
		Markets []int  `json:"Markets"`
	}{}

	if err = json.Unmarshal(data, &alias); err != nil {
		return err
	}

	e.Id = alias.Id
	e.Name = alias.Name
	e.Time, err = time.Parse("2006-01-02:15:04:05Z07:00", alias.Time)
	e.Markets = alias.Markets

	return err
}

func EventIsEqual(a *Event, b *Event) bool {
	if a.Id != b.Id || a.Name != b.Name || a.Time != b.Time {
		return false
	}
	if len(a.Markets) != len(b.Markets) {
		return false
	}
	for i, market := range a.Markets {
		if market != b.Markets[i] {
			return false
		}
	}
	return true
}

type EventRepository struct {
	BaseURL *url.URL
	pointer int
}

func NewEventRepository(baseURL string) (*EventRepository, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	u.Path = uriPath
	return &EventRepository{BaseURL: u}, nil
}

func (e *EventRepository) CheckForNewItems() ([]int, error) {
	var items []int

	err := get(e.BaseURL.String(), interface{}(&items))
	if err != nil {
		return nil, err
	}

	// assumes:
	//	- items may appear in the array in any order
	// 	- new items will have a higher ID than perviously published items
	// 	- no duplicates
	// 	- no updates to previously published items - updates will be published as new events with new ids
	newItems := items[:0]
	newPointer := e.pointer
	for _, item := range items {
		if item > e.pointer {
			newItems = append(newItems, item)
			if item > newPointer {
				newPointer = item
			}
		}
	}
	e.pointer = newPointer

	return newItems, nil
}

func (e *EventRepository) GetByID(id int) (*Event, error) {
	var event Event

	err := get(fmt.Sprintf("%s/%d", e.BaseURL.String(), id), interface{}(&event))
	if err != nil {
		return nil, err
	}

	return &event, nil
}

type Market struct {
	Id      string   `json:"id"`
	Type    string   `json:"type"`
	Options []Option `json:"options"`
}

type Option struct {
	Id   string `json:"id"`
	Name string `json:"name"`
	Odds string `json:"odds"`
}

func MarketIsEqual(a *Market, b *Market) bool {
	if a.Id != b.Id || a.Type != b.Type {
		return false
	}
	if len(a.Options) != len(b.Options) {
		return false
	}
	for i, option := range a.Options {
		if option.Id != b.Options[i].Id ||
			option.Name != b.Options[i].Name ||
			option.Odds != b.Options[i].Odds {
			return false
		}
	}
	return true
}

type MarketRepository struct {
	BaseURL *url.URL
}

func NewMarketRepository(baseURL string) (*MarketRepository, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	u.Path = "football/markets"
	return &MarketRepository{BaseURL: u}, nil
}

func (m *MarketRepository) GetByID(id int) (*Market, error) {
	var market Market

	err := get(fmt.Sprintf("%s/%d", m.BaseURL.String(), id), interface{}(&market))
	if err != nil {
		return nil, err
	}

	return &market, nil
}

func get(uri string, resource interface{}) error {
	resp, err := http.Get(uri)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := json.NewDecoder(resp.Body).Decode(&resource); err != nil {
		return err
	}

	return nil
}
