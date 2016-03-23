package signalfx

import (
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/outputs"
	"github.com/influxdata/telegraf/testutil"
	"github.com/signalfx/golib/datapoint/dptest"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestSignalFxValConvert(t *testing.T) {
	params := []struct {
		val      interface{}
		expected string
		err      error
	}{
		{
			val:      int(5),
			expected: "5",
		},
		{
			val:      int32(2),
			expected: "2",
		},
		{
			val:      int64(3),
			expected: "3",
		},
		{
			val:      float32(4),
			expected: "4",
		},
		{
			val:      float64(5),
			expected: "5",
		},
		{
			val:      false,
			expected: "0",
		},
		{
			val:      true,
			expected: "1",
		},
		{
			val:      time.Unix(0, 0),
			expected: "0",
		},
		{
			val: "inconceivable",
			err: errUndetermineableType,
		},
	}
	for _, param := range params {
		val, err := valFromField(param.val)
		if param.err != nil {
			require.Equal(t, err, param.err)
			continue
		}
		require.Equal(t, val.String(), param.expected)
	}
}

func TestSignalFxClient(t *testing.T) {
	clientFunc := outputs.Outputs["signalfx"]
	require.NotNil(t, clientFunc)
	client := clientFunc().(*signalFx)
	require.Equal(t, errNoTokenSet, client.Connect())
	client.Token = "abc"
	client.Endpoint = "testing"
	client.Debug = true
	require.Nil(t, client.Connect())
	require.NotEmpty(t, client.Description())
	require.NotEmpty(t, client.SampleConfig())
	require.Nil(t, client.Close())

	require.Nil(t, client.Write(nil))

	testSink := dptest.NewBasicSink()
	testSink.Resize(2)
	client.client = testSink

	m2, err := telegraf.NewMetric(
		"test2",
		nil,
		map[string]interface{}{"cpuusage": 10},
		time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
	)
	require.Nil(t, err)

	m3, err := telegraf.NewMetric(
		"test3",
		nil,
		map[string]interface{}{"inconceivable": func() {}},
		time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
	)
	require.Nil(t, err)

	require.Nil(t, client.Write([]telegraf.Metric{
		testutil.TestMetric(1),
		m2,
		m3,
	}))
	points := <-testSink.PointsChan
	require.Equal(t, 2, len(points))

	require.Equal(t, "test1", points[0].Metric)
	require.Equal(t, "1", points[0].Value.String())
	require.Equal(t, time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC), points[0].Timestamp)

	require.Equal(t, "test2.cpuusage", points[1].Metric)
	require.Equal(t, "10", points[1].Value.String())
	require.Equal(t, time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC), points[1].Timestamp)
}
