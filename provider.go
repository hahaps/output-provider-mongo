package main

import (
	"github.com/hahaps/common-provider/src/output"
	"github.com/hahaps/output-provider-mongo/src"
	"log"
	"strings"
)

var VERSION string = "v0.1"

type Version struct {}

func (Version)Check(version string, matched *bool) error {
	*matched = strings.Compare(VERSION, version) == 0
	return nil
}

func main() {
	if err := output.RunProvider(Version{}, src.MongoStore{}); err != nil {
		log.Fatal(err)
	}
}
