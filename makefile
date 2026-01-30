VERSION?=latest
.PHONY:upgrade-fun
upgrade-fun:
	go get github.com/tychoish/fun@$(VERSION)
	for i in $(shell find . -name "go.mod"); do pushd $$(dirname $$i); echo $(dirname $i); go get github.com/tychoish/fun@$(VERSION); go mod tidy; popd; done

go-mod-tidy:
	go mod tidy
	for i in $(shell find . -name "go.mod"); do pushd $$(dirname $$i); echo $(dirname $i); go mod tidy; popd; done
