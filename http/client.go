package http

import (
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	inFlight = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "http_outbound_requests_in_flight",
			Help: "In Flight Outbound HTTP requests.",
		},
		[]string{"method", "host"},
	)
	requestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_outbound_requests_total",
			Help: "Counter of successful Outbound HTTP requests.",
		},
		[]string{"method", "host"},
	)
	requestStatus = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_outbound_requests_status_total",
			Help: "Counter of successful Outbound HTTP requests by status code.",
		},
		[]string{"method", "host", "code"},
	)
	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_outbound_request_duration_seconds",
			Help:    "Histogram of latencies for Outbound HTTP requests.",
			Buckets: []float64{.001, .005, .01, .05, .1, .2, .4, 1, 3, 8, 20, 60, 120},
		},
		[]string{"method", "host"},
	)
)

func init() {
	prometheus.MustRegister(inFlight, requestCounter, requestStatus, requestDuration)
}

type Client struct {
	*http.Client
}

func New(client *http.Client) *Client {
	return &Client{Client: client}
}

func (c *Client) Do(request *http.Request) (*http.Response, error) {
	start := time.Now()
	inFlight.WithLabelValues(strings.ToLower(request.Method), request.URL.Host).Inc()
	defer func(request *http.Request, start time.Time) {
		inFlight.WithLabelValues(strings.ToLower(request.Method), request.URL.Host).Dec()
		requestDuration.WithLabelValues(strings.ToLower(request.Method), request.URL.Host).Observe(time.Since(start).Seconds())
	}(request, start)

	response, err := c.Client.Do(request)
	requestStatus.WithLabelValues(strings.ToLower(request.Method), request.URL.Host, strconv.Itoa(response.StatusCode)).Inc()
	return response, err
}

func (c *Client) Get(url string) (*http.Response, error) {
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	return c.Do(request)
}

func (c *Client) Head(url string) (*http.Response, error) {
	request, err := http.NewRequest(http.MethodHead, url, nil)
	if err != nil {
		return nil, err
	}
	return c.Do(request)
}

func (c *Client) Post(url, contentType string, body io.Reader) (*http.Response, error) {
	request, err := http.NewRequest(http.MethodPost, url, body)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", contentType)
	return c.Do(request)
}

func (c *Client) PostForm(url string, data url.Values) (*http.Response, error) {
	return c.Post(url, "application/x-www-form-urlencoded", strings.NewReader(data.Encode()))
}
