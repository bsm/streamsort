default: vet test

vet:
	go vet ./...

test:
	go test ./...

bench:
	go test ./... -bench=. -benchmem -benchtime=30s

errcheck:
	errcheck ./...

README.md: README.md.tpl $(wildcard *.go)
	becca -package $(subst $(GOPATH)/src/,,$(PWD))
