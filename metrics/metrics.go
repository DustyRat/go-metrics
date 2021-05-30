package metrics

import (
	"runtime"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var funcHistogram *prometheus.HistogramVec

func init() {
	funcHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "func_process_time",
			Help:    "Histogram of function run times for function",
			Buckets: []float64{.001, .005, .01, .05, .1, .2, .4, 1, 3, 8, 20, 60, 120},
		},
		[]string{"file", "func", "type"},
	)
	prometheus.MustRegister(funcHistogram)
}

func ObserveCaller(label string, start time.Time) {
	if pc, file, _, ok := runtime.Caller(1); ok {
		if function := runtime.FuncForPC(pc); function != nil {
			funcHistogram.WithLabelValues(filename(file), funcname(function.Name()), label).Observe(time.Since(start).Seconds())
		}
	}
}

func filename(name string) string {
	i := strings.LastIndex(name, "/")
	return name[i+1:]
}

func funcname(name string) string {
	i := strings.LastIndex(name, "/")
	name = name[i+1:]
	i = strings.Index(name, ".")
	return name[i+1:]
}
