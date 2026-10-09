package main

import (
	"os"

	"github.com/davidcollom/komodor-metrics-exporter/cmd"
)

// version is set by GoReleaser's -X main.version ldflag.
var version = "dev"

func main() {
	if err := cmd.Execute(version); err != nil {
		os.Exit(1)
	}
}
