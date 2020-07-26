lint:
	revive -formatter friendly -config revive.toml *.go
	revive -formatter friendly -config revive.toml service/*.go

build:
	go build -o app.out -v service/app.go

test:
	go test -v .

clean:
	rm app

format:
	gofmt -s -l -w *.go
	gofmt -s -l -w service/*.go

.PHONY: test clean build lint format