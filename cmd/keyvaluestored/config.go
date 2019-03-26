package main

import (
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Config the application's configuration structure
type Config struct {
	RedisListenPort         int
	StaticDiscovery         string
	LocalConnection         string
	DefaultWriteConsistency string
	DefaultReadConsistency  string
	Backend                 string
	Profiling               bool
}

// LoadConfig loads the config from a file if specified, otherwise from the environment
func LoadConfig(cmd *cobra.Command, envPrefix string) (*Config, error) {
	// Setting defaults for this application
	viper.SetDefault("redisListenPort", 6380)
	viper.SetDefault("backend", "redis")
	viper.SetDefault("staticDiscovery", "")
	viper.SetDefault("localConnection", "")
	viper.SetDefault("defaultWriteConsistency", "majority")
	viper.SetDefault("defaultReadConsistency", "majority")
	viper.SetDefault("profiling", false)

	// Read Config from ENV
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.SetEnvPrefix(envPrefix)
	viper.AutomaticEnv()

	// Read Config from Flags
	err := viper.BindPFlags(cmd.Flags())
	if err != nil {
		return nil, err
	}

	// Read Config from file
	if configFile, err := cmd.Flags().GetString("config-file"); err == nil && configFile != "" {
		viper.SetConfigFile(configFile)

		if err := viper.ReadInConfig(); err != nil {
			return nil, err
		}
	}

	var config Config

	err = viper.Unmarshal(&config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}
