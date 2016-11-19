package ui

import (
	"bytes"
	"io"
	"net/http"
	"path/filepath"

	log "github.com/Sirupsen/logrus"
)

var Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
	fp := filepath.Join("zipkin-ui/static", req.URL.Path)

	info, err := AssetInfo(fp)
	if err != nil {
		log.Warn("Could not get file info: ", err)
		fp = "zipkin-ui/static/index.html"
		info, _ = AssetInfo(fp)
	}

	file, err := Asset(fp)
	if err != nil {
		if err != io.EOF {
			log.Warn("Could not get file: ", err)
		}
		w.WriteHeader(http.StatusNotFound)
		return
	}

	http.ServeContent(w, req, info.Name(), info.ModTime(), bytes.NewReader(file))
})
