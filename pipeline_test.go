package main

import (
	"encoding/json"
	"github.com/james-bowman/pipeline/datasrc"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"
)

var jsonSrcEvents = map[int]string{
	1: `{
		"id": 1,
		"name": "Southampton v Bournemouth",
		"time": "2017-08-20:15:00:00Z",
		"markets": [
			101,
			102
		]
	}`,
	2: `{
		"id": 2,
		"name": "Arsenal v Bournemouth",
		"time": "2017-08-20:15:00:00Z",
		"markets": [
			102
		]
	}`,
	3: `{
		"id": 3,
		"name": "Southampton v Arsenal",
		"time": "2017-08-21:15:00:00Z",
		"markets": [
			101
		]
	}`,
}

var jsonSrcMarkets = map[int]string{
	101: `{
		"id": "101",
		"type": "win-draw-win",
		"options": [
			{
				"id": "10101",
				"name": "Southampton",
				"odds": "3/5"
			},
			{
				"id": "10102",
				"name": "Draw",
				"odds": "4/5"
			},
			{
				"id": "10103",
				"name": "Bournemouth",
				"odds": "5/1"
			}
		]
	}`,
	102: `{
		"id": "102",
		"type": "win-draw-win",
		"options": [
			{
				"id": "10201",
				"name": "Southampton",
				"odds": "3/5"
			},
			{
				"id": "10202",
				"name": "Draw",
				"odds": "4/5"
			},
			{
				"id": "10203",
				"name": "Bournemouth",
				"odds": "5/1"
			}
		]
	}`,
}

var expectedEvents = map[int]Event{
	1: Event{
		Id:   "1",
		Name: "Southampton v Bournemouth",
		Time: time.Date(2017, 8, 20, 15, 00, 00, 00, time.UTC), // "2017-08-19:15:00:00Z"
		Markets: []Market{
			{
				Id:   "101",
				Type: "win-draw-win",
				Options: []Option{
					{Id: "10101", Name: "Southampton", Num: 1, Den: 5},
					{Id: "10102", Name: "Draw", Num: 3, Den: 5},
					{Id: "10103", Name: "Bournemouth", Num: 4, Den: 5},
				},
			},
			{
				Id:   "102",
				Type: "win-draw-win",
				Options: []Option{
					{Id: "10201", Name: "Southampton", Num: 1, Den: 5},
					{Id: "10202", Name: "Draw", Num: 3, Den: 5},
					{Id: "10203", Name: "Bournemouth", Num: 4, Den: 5},
				},
			},
		},
	},
	2: Event{
		Id:   "2",
		Name: "Arsenal v Bournemouth",
		Time: time.Date(2017, 8, 20, 15, 00, 00, 00, time.UTC), // "2017-08-19:15:00:00Z"
		Markets: []Market{
			{
				Id:   "102",
				Type: "win-draw-win",
				Options: []Option{
					{Id: "10201", Name: "Southampton", Num: 1, Den: 5},
					{Id: "10202", Name: "Draw", Num: 3, Den: 5},
					{Id: "10203", Name: "Bournemouth", Num: 4, Den: 5},
				},
			},
		},
	},
	3: Event{
		Id:   "1",
		Name: "Southampton v Arsenal",
		Time: time.Date(2017, 8, 21, 15, 00, 00, 00, time.UTC), // "2017-08-19:15:00:00Z"
		Markets: []Market{
			{
				Id:   "101",
				Type: "win-draw-win",
				Options: []Option{
					{Id: "10101", Name: "Southampton", Num: 1, Den: 5},
					{Id: "10102", Name: "Draw", Num: 3, Den: 5},
					{Id: "10103", Name: "Bournemouth", Num: 4, Den: 5},
				},
			},
		},
	},
}

func testingHttpHandler(t *testing.T, out chan Event, ids ...int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/football/events" {
			// Get new Event IDs
			w.Header().Set("Content-Type", "application/json")
			idJson, _ := json.Marshal(ids)
			w.WriteHeader(http.StatusOK)
			w.Write(idJson)
		} else if strings.HasPrefix(r.URL.Path, "/football/events/") {
			// Get Event
			w.Header().Set("Content-Type", "application/json")
			id, _ := strconv.Atoi(path.Base(r.URL.Path))
			w.Write([]byte(jsonSrcEvents[id]))
		} else if strings.HasPrefix(r.URL.Path, "/football/markets/") {
			// Get Market
			w.Header().Set("Content-Type", "application/json")
			id, _ := strconv.Atoi(path.Base(r.URL.Path))
			w.Write([]byte(jsonSrcMarkets[id]))
		} else if r.URL.Path == "/event" {
			// Post Event to Store
			var event Event
			defer r.Body.Close()
			received, _ := ioutil.ReadAll(r.Body)
			json.Unmarshal(received, &event)
			out <- event
		} else {
			// invalid URL
			t.Errorf("Unexpected Request URI for feeds: '%s'\n", r.URL.Path)
		}
	}
}

func TestPipeline(t *testing.T) {
	tests := []struct {
		idsIn  []int
		idsOut []int
	}{
		{
			idsIn:  []int{1, 2, 3, 4, 5},
			idsOut: []int{1, 2, 3},
		},
		{
			idsIn:  []int{1},
			idsOut: []int{1},
		},
		{
			idsIn:  []int{1, 2, 3},
			idsOut: []int{1, 2, 3},
		},
	}

	for ti, test := range tests {
		out := make(chan Event, len(test.idsOut))
		ts := httptest.NewServer(http.HandlerFunc(testingHttpHandler(t, out, test.idsIn...)))
		defer ts.Close()

		events, err := datasrc.NewEventRepository(ts.URL)
		if err != nil {
			t.Errorf("Test %d: Failed to create EventRepository because %v", ti, err)
		}

		markets, err := datasrc.NewMarketRepository(ts.URL)
		if err != nil {
			t.Errorf("Test %d: Failed to create MarketRepository because %v", ti, err)
		}

		feed := NewFeed(events, time.Duration(1*time.Millisecond))
		stream := NewEventStream(feed.NewItems(), events, markets)

		store, err := NewEventStore(ts.URL)
		if err != nil {
			t.Errorf("Test %d: Failed to create Store because %v", ti, err)
		}

		sink := NewEventSink(stream.Events(), store)
		go sink.ProcessAll()

		for n := 0; n < len(test.idsOut); n++ {
			select {
			case event := <-out:
				id, _ := strconv.Atoi(event.Id)
				if id != test.idsOut[n] {
					t.Errorf("Test %d: Expected ID %d but received %d\n", ti, test.idsOut[n], id)
				}
			case <-time.After(1 * time.Minute):
				// timeout
				t.Errorf("Test %d: Timeout whilst waiting for event from event stream\n", ti)
				break
			}
		}
		feed.Close()
	}
}
