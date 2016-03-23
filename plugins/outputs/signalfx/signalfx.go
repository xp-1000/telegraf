package signalfx

import (
	"errors"
	"fmt"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/outputs"
	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/sfxclient"
	"golang.org/x/net/context"
	"log"
	"runtime"
	"time"
)

type signalFx struct {
	Token    string
	Timeout  internal.Duration
	Endpoint string
	Debug    bool

	client sfxclient.Sink
}

var sampleConfig = `
  ## SignalFx API Token
  token = "XYZ-ABC" # required.

  ## Connection timeout.
  # timeout = "5s"
  ## Endpoint
  # endpoint = "https://ingest.signalfx.com/v2/datapoint"
`

var errNoTokenSet = errors.New("field token required to be set for SignalFx plugin")
var errUndetermineableType = errors.New("undeterminable type")

func (d *signalFx) Connect() error {
	if d.Token == "" {
		return errNoTokenSet
	}
	if d.Timeout.Duration.Nanoseconds() == 0 {
		d.Timeout.Duration = time.Second * 3
	}
	httpClient := sfxclient.NewHTTPDatapointSink()
	httpClient.Client.Timeout = d.Timeout.Duration
	httpClient.AuthToken = d.Token
	if d.Endpoint != "" {
		httpClient.Endpoint = d.Endpoint
	}

	// TODO: I wish telegraf had a way for me to discover the running Version here.  It's stored
	//       in package main which I can't access from here because that's a circular dep
	telegrafVersion := "unknown(v1)"
	httpClient.UserAgent = fmt.Sprintf("telegraf/%s (gover %s)", telegrafVersion, runtime.Version())
	d.client = httpClient
	if d.Debug {
		log.Printf("[signalfx] Write enabled to %s", httpClient.Endpoint)
	}
	return nil
}

func valFromField(v interface{}) (datapoint.Value, error) {
	switch d := v.(type) {
	case int:
		return datapoint.NewIntValue(int64(d)), nil
	case int32:
		return datapoint.NewIntValue(int64(d)), nil
	case int64:
		return datapoint.NewIntValue(d), nil
	case float32:
		return datapoint.NewFloatValue(float64(d)), nil
	case float64:
		return datapoint.NewFloatValue(d), nil
	case bool:
		if d {
			return datapoint.NewIntValue(1), nil
		}
		return datapoint.NewIntValue(0), nil
	case time.Time:
		return datapoint.NewIntValue(d.UnixNano()), nil
	default:
		return nil, errUndetermineableType
	}
}

func (d *signalFx) Write(metrics []telegraf.Metric) error {
	if len(metrics) == 0 {
		return nil
	}

	var points []*datapoint.Datapoint
	for _, m := range metrics {
		for k, v := range m.Fields() {
			val, err := valFromField(v)
			if err != nil {
				//log this
				continue
			}
			metricName := m.Name()
			if k != "value" {
				metricName += "." + k
			}
			// TODO: Type Gauge is ambiguous here.  Unsure which type to use
			points = append(points, datapoint.New(metricName, m.Tags(), val, datapoint.Gauge, m.Time()))
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), d.Timeout.Duration)
	defer cancel()

	return d.client.AddDatapoints(ctx, points)
}

func (d *signalFx) SampleConfig() string {
	return sampleConfig
}

func (d *signalFx) Description() string {
	return "Configuration for SignalFx API to send metrics to."
}

func (d *signalFx) Close() error {
	return nil
}

func init() {
	outputs.Add("signalfx", func() telegraf.Output {
		return &signalFx{}
	})
}
