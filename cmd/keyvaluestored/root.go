package main

import (
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "keyvaluestored <subcommand>",
	Short: "provides a gRPC proxy to redis, etc.",
	Long:  `provides a gRPC proxy to redis, etc. intended to be used as a sidecar to redis`,
	Run:   nil,
}

func init() {
	cobra.OnInitialize()
	rootCmd.PersistentFlags().StringP("config-file", "c", "", "Path to the config file (eg ./config.yaml) [Optional]")
}
