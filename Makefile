default: vet test

vet:
	go vet ./...

test:
	go test ./...

test-race:
	go test ./... -race

bench:
	go test ./... -run=NONE -bench=. -benchmem -benchtime=30s

bench-race:
	go test ./... -run=NONE -bench=. -race

errcheck:
	errcheck ./...

README.md: README.md.tpl $(wildcard *.go)
	becca -package $(subst $(GOPATH)/src/,,$(PWD))
