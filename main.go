package main

import (
	"flag"
	"log"
	"net/url"
	"strings"
	"time"

	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/joeshaw/iso8601"
)

const (
	typeHeartbeat = "heartbeat"
	typeTicker    = "ticker"
	typeLevel2    = "l2update"

	writeWait = 10 * time.Second
	readWait  = 60 * time.Second
)

// Client to a websocket connection
type Client struct {
	Connection *websocket.Conn
}

// Msg struct for all response data
// Master list of all json fields across channels using omitempty to only populate relevant fields per channel
type Msg struct {
	Type        string          `json:"type"`
	Sequence    int64           `json:"sequence,omitempty"`
	LastTradeId int64           `json:"last_trade_id,omitempty"`
	ProductId   string          `json:"product_id,omitempty"`
	Time        time.Time       `json:"time,omitempty"`
	TradeId     int64           `json:"trade_id,omitempty"`
	Price       string          `json:"price,omitempty"`
	Side        string          `json:"side,omitempty"`
	LastSize    string          `json:"last_size,omitempty"`
	BestBid     string          `json:"best_bid,omitempty"`
	BestAsk     string          `json:"best_ask,omitempty"`
	Changes     [][]interface{} `json:"changes,omitempty"`
	ReceivedAt  string          `json:"received_at"`
}

// Request struct to wrap up parameters to WriteMessage
type Request struct {
	Type      string   `json:"type"`
	ProductId []string `json:"product_ids"`
	Channels  []string `json:"channels"`
}

// Logger struct used to write to file
type Logger struct {
	mutex *sync.Mutex
	file  *os.File
}

// Flags are all pointers
var addr = flag.String("addr", "ws-feed.gdax.com", "http service address")
var fileName = flag.String("file", "gdax.txt", "write all received messages to a file")

// Allow user to pick which channels they want to subscribe to: all, level2, ticker or heartbeat
// Separate multiple channels by commas - e.g. -channels=ticker,heartbeat
var channels = flag.String("channels", "ticker", "which channels do you want to subscribe to from feed")

// Allow user to select currency pair they want to subscribe to: e.g. ETH-USD, BTC-USD, etc...
// Separate multiple ccys by commas - e.g. -ccy=BTC-USD,ETH-USD
var ccyPair = flag.String("ccy", "ETH-USD", "which currency pair do you want to subscribe to")

func NewFromRequest(urlStr string) (*Client, error) {
	c, _, err := websocket.DefaultDialer.Dial(urlStr, nil)
	if err != nil {
		return nil, err
	}

	c.SetWriteDeadline(time.Now().Add(writeWait))

	// Populate Request struct from flags
	r := Request{
		Type:      "subscribe",
		ProductId: strings.Split(*ccyPair, ","),
		Channels:  strings.Split(*channels, ","),
	}

	// Marshal Request struct into json-encoded version
	rtm, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}

	log.Printf("Request %s", string(rtm))

	// Pass in json-encoded Request struct to make request more generic and avoid having to type everything out like:
	// c.WriteMessage(websocket.TextMessage, []byte(`{"type":"subscribe","product_ids":["BTC-USD","ETH-USD"],"channels":["ticker","heartbeat","level2"]}`))
	c.WriteMessage(websocket.TextMessage, rtm)

	return &Client{c}, nil
}

func main() {
	flag.Parse()

	u := url.URL{Scheme: "wss", Host: *addr}
	log.Printf("connecting to %s", u.String())
	log.Printf("subscribing to %s", *channels)

	c, err := NewFromRequest(u.String())
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer c.Connection.Close()

	ch := make(chan []byte)
	logger, err := newLogger(*fileName)
	if err != nil {
		log.Fatal("logger:", err)
	}
	go dispatcher(c, ch, logger)
	reader(c, ch)
	close(ch)
}

func dispatcher(c *Client, ch chan []byte, logger *Logger) {
	defer func() {
		c.Connection.Close()
	}()
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case m, ok := <-ch:
			if !ok {
				log.Println("MESSAGING: Could not receive event")
				c.Connection.SetWriteDeadline(time.Now().Add(writeWait))
				c.Connection.WriteMessage(websocket.CloseMessage, nil)
				return
			}
			if err := processWrite(m, logger); err != nil {
				log.Printf("MESSAGING: Error in client: %v", err)
				return
			}
		case t := <-ticker.C:
			c.Connection.SetWriteDeadline(time.Now().Add(writeWait))
			err := c.Connection.WriteMessage(websocket.TextMessage, []byte(t.String()))
			if err != nil {
				log.Printf("MESSAGING: Could not write message, err: %v", err)
				return
			}
		}
	}
}

func reader(c *Client, ch chan []byte) {
	defer func() {
		c.Connection.Close()
	}()

	for {
		c.Connection.SetReadDeadline(time.Now().Add(readWait))
		_, m, err := c.Connection.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway) {
				log.Printf("error: %v", err)
			}
			break
		}
		ch <- m
		//fmt.Println("reading...")
	}
}

func processWrite(message []byte, logger *Logger) error {
	m := &Msg{}
	err := json.Unmarshal(message, m)
	if err != nil {
		return err
	}
	m.ReceivedAt = time.Now().Format(iso8601.Format)
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}

	if m.Type == typeHeartbeat || m.Type == typeTicker || m.Type == typeLevel2 {
		logger.Write(data)
		log.Printf("Writing %s", string(data))
	}

	return nil
}

func newLogger(fileName string) (*Logger, error) {
	f, err := os.Create(fileName)
	if err != nil {
		return nil, err
	}
	return &Logger{file: f, mutex: &sync.Mutex{}}, nil
}

func (w *Logger) Write(b []byte) {
	w.mutex.Lock()
	w.file.WriteString(fmt.Sprintf("%s\n", string(b)))
	w.mutex.Unlock()
}
