FROM golang:1.8.0
RUN apt-get update && apt-get install -y file jq && \
	rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
RUN go clean -i net && \
	go install -tags netgo std && \
	go install -race -tags netgo std
RUN go get -tags netgo \
		github.com/fzipp/gocyclo \
		github.com/golang/lint/golint \
		github.com/kisielk/errcheck \
		github.com/client9/misspell/cmd/misspell \
		github.com/jteeuwen/go-bindata/go-bindata \
		gopkg.in/mvdan/sh.v1/cmd/shfmt && \
	rm -rf /go/pkg /go/src
COPY build.sh /
ENTRYPOINT ["/build.sh"]
