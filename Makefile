default: test

test:
	go test ./...

bench:
	go test ./... -bench=. -benchmem -benchtime=30s

README.md: README.md.tpl $(wildcard *.go)
	becca -package $(subst $(GOPATH)/src/,,$(PWD))
