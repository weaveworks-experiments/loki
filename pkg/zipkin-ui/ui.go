package ui

import (
	"bytes"
	"html/template"
	"io"
	"net/http"
	"path/filepath"
	"strings"

	log "github.com/Sirupsen/logrus"
)

type UI struct {
	PathPrefix string
}

func (ui UI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fp := filepath.Join("pkg/zipkin-ui/static", r.URL.Path)

	info, err := AssetInfo(fp)
	if err != nil && strings.Contains(fp, ".") {
		w.WriteHeader(http.StatusNotFound)
		return
	} else if err != nil {
		ui.index(w, r)
		return
	}

	file, err := Asset(fp)
	if err != nil {
		if err != io.EOF {
			log.Warn("Could not get file: ", err)
		}
		w.WriteHeader(http.StatusNotFound)
		return
	}

	http.ServeContent(w, r, info.Name(), info.ModTime(), bytes.NewReader(file))
}

func (ui UI) index(w http.ResponseWriter, req *http.Request) {
	raw := `<!DOCTYPE html>
<html>
  <head>
		<base href="/api/loki">
    <meta charset="UTF-8">
    <title>Webpack App</title>
  <link href="{{ .PathPrefix }}/app-e5d412b21f914bbdf087.min.css" rel="stylesheet"></head>
  <body>
  <script type="text/javascript" src="{{ .PathPrefix }}/app-e5d412b21f914bbdf087.min.js"></script></body>
</html>
	`
	tmpl, err := template.New("index").Parse(raw)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := tmpl.Execute(w, struct {
		PathPrefix string
	}{
		PathPrefix: ui.PathPrefix,
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
