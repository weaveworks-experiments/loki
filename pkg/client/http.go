package loki

import (
	"html/template"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"

	"github.com/weaveworks-experiments/loki/pkg/model"
	"github.com/weaveworks-experiments/loki/pkg/storage"
)

func contains(haystack []string, needle string) bool {
	for _, hay := range haystack {
		if hay == needle {
			return true
		}
	}
	return false
}

func (c *Collector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	spans := model.Spans{
		Spans: c.gather(),
	}

	encodings := strings.Split(r.Header.Get("Accept-Encoding"), ",")
	var err error
	if contains(encodings, "text/html") {
		err = encodeHTML(spans, w)
	} else {
		err = encodeProto(spans, w)
	}

	if err != nil {
		log.Printf("error writing spans: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func Handler() http.Handler {
	return globalCollector
}

func encodeProto(spans model.Spans, w io.Writer) error {
	buf, err := spans.Marshal()
	if err != nil {
		return nil
	}
	_, err = w.Write(buf)
	return err
}

const tpl = `
<!DOCTYPE html>
<html>
	<head>
		<meta charset="UTF-8">
		<title>Traces</title>
	</head>
	<body>
		<h1>Traces</h1>
		<table width="100%" border="1">
			<thead>
				<tr>
					<th>Time</th>
          <th>Duration</th>
					<th>ID</th>
					<th>Path</th>
				</tr>
			</thead>
			<tbody>
				{{ range .Traces }}
				<tr>
					<td>{{ .MinTimestamp }}</td>
          <td>{{ call .MaxTimestamp.Sub .MinTimestamp }}</td>
					<td></td>
					<td></td>
				</tr>
				{{ end }}
			</tbody>
		</table>
	</body>
</html>`

var tmpl *template.Template

func init() {
	var err error
	tmpl, err = template.New("webpage").Parse(tpl)
	if err != nil {
		panic(err)
	}
}

func encodeHTML(spans model.Spans, w io.Writer) error {
	traces := map[uint64]*storage.Trace{}
	for _, span := range spans.Spans {
		trace, ok := traces[span.TraceId]
		if !ok {
			traces[span.TraceId] = storage.NewTrace(span)
		} else {
			trace.AddSpan(span)
		}
	}

	sorted := []storage.Trace{}
	for _, trace := range traces {
		sorted = append(sorted, *trace)
	}
	sort.Sort(storage.ByMinTimestamp(sorted))

	return tmpl.Execute(w, struct {
		Traces []storage.Trace
	}{
		Traces: sorted,
	})
}
