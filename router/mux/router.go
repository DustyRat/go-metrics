package router

import (
	"net/http"

	_ "net/http/pprof" // debug

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const MetricsRouteName = "METRICS"

var (
	inFlight = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "http_requests_in_flight",
			Help: "In Flight HTTP requests.",
		},
	)
	requestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "counter of HTTP requests.",
		},
		[]string{"handler", "code", "method"},
	)
	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Histogram of latencies for HTTP requests.",
			Buckets: []float64{.01, .05, .1, .2, .4, 1, 3, 8, 20, 60, 120},
		},
		[]string{"handler", "code", "method"},
	)
)

func init() {
	prometheus.MustRegister(inFlight, requestCounter, requestDuration)
}

// Router ...
type Router struct {
	*mux.Router
}

// New ...
func New(mux *mux.Router) *Router {
	router := &Router{
		Router: mux,
	}
	router.Handle("/metrics", promhttp.Handler()).Name(MetricsRouteName)
	return router
}

// Handle implements http.Handler.
func (r *Router) Handle(path string, handler http.Handler) *mux.Route {
	handler = recovery(handler)
	return r.Router.Handle(path, handler)
}

// HandleFunc implements http.Handler.
func (r *Router) HandleFunc(path string, f http.HandlerFunc) *mux.Route {
	return r.Handle(path, f)
}

// HandleWithMetrics implements http.Handler wrapping handler with
func (r *Router) HandleWithMetrics(path string, handler http.Handler) *mux.Route {
	handler = recovery(handler)
	return r.Router.Handle(handle(path, handler))
}

// HandleFuncWithMetrics implements http.Handler wrapping handler func with
func (r *Router) HandleFuncWithMetrics(path string, f http.HandlerFunc) *mux.Route {
	return r.HandleWithMetrics(path, f)
}

func recovery(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			defer r.Body.Close()
			ctx := r.Context()

			if r := recover(); r != nil {
				var err error
				switch t := r.(type) {
				case string:
					err = errors.New(t)
				case error:
					err = errors.WithStack(t)
				default:
					err = errors.New("unknown error")
				}
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			} else if err := ctx.Err(); err != nil {
				// Use nginx's non-standard response code for metrics
				// 499: Client Closed Request
				http.Error(w, err.Error(), 499)
				return
			}
		}()
		h.ServeHTTP(w, r)
	})
}

func handle(path string, handler http.Handler) (string, http.Handler) {
	return path,
		promhttp.InstrumentHandlerCounter(requestCounter.MustCurryWith(prometheus.Labels{"handler": path}),
			promhttp.InstrumentHandlerInFlight(inFlight,
				promhttp.InstrumentHandlerDuration(requestDuration.MustCurryWith(prometheus.Labels{"handler": path}),
					handler,
				),
			),
		)
}
