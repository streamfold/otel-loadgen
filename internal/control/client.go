package control

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"go.uber.org/zap"
)

// Client is a client for the control server
type Client struct {
	endpointUrl *url.URL
	log         *zap.Logger
	msgCh       chan Control
	wg          sync.WaitGroup
	client      *http.Client
}

// NewClient creates a new control server client
func NewClient(endpoint string, log *zap.Logger) (*Client, error) {
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		endpoint = fmt.Sprintf("http://%s", endpoint)
	}

	endpointUrl, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}

	return &Client{
		endpointUrl: endpointUrl,
		log:         log,
		msgCh:       make(chan Control, 100),
		client:      &http.Client{},
	}, nil
}

// MessageChannel returns the channel for sending message ranges
func (c *Client) MessageChannel() chan<- Control {
	return c.msgCh
}

// Start begins processing message ranges and sending them to the control server
func (c *Client) Start() {
	c.wg.Add(1)
	go c.processMessages()
	c.log.Info("Control client started", zap.String("endpoint", c.endpointUrl.String()))
}

// Stop gracefully stops the client
func (c *Client) Stop() {
	c.log.Info("Stopping control client")
	close(c.msgCh)
	c.wg.Wait()
	c.log.Info("Control client stopped")
}

func (c *Client) processMessages() {
	defer c.wg.Done()

	for ctrl := range c.msgCh {
		mr := ctrl.Range
		if err := c.postMessageRange(ctrl.Type, mr); err != nil {
			c.log.Error("failed to post message range",
				zap.Error(err),
				zap.String("generator_id", mr.GeneratorID),
				zap.Uint64("start_id", mr.StartID),
				zap.Uint("range_len", mr.RangeLen),
			)
		} else {
			c.log.Debug("posted message range",
				zap.String("generator_id", mr.GeneratorID),
				zap.Uint64("start_id", mr.StartID),
				zap.Uint("range_len", mr.RangeLen),
			)
		}

	}
}

func (c *Client) postMessageRange(msgType ControlType, mr MessageRange) error {
	pub := ControlMessage{
		GeneratorID: mr.GeneratorID,
		Timestamp:   mr.Timestamp,
		StartID:     mr.StartID,
		RangeLen:    mr.RangeLen,
	}

	data, err := json.Marshal(pub)
	if err != nil {
		return fmt.Errorf("failed to marshal published message: %w", err)
	}

	url := fmt.Sprintf("%s/api/message_range", c.endpointUrl.String())
	method := http.MethodPost
	if msgType == ControlTypeUpdate {
		method = http.MethodPut
	}

	req, err := http.NewRequestWithContext(context.Background(), method, url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}
