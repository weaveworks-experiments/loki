package config

import (
	"gopkg.in/yaml.v2"

	"github.com/prometheus/promtheus/config"
)

// Config is the top-level configuration for Loki's config files.
type Config struct {
	ScrapeConfigs []*config.ScrapeConfig `yaml:"scrape_configs,omitempty"`

	// Catches all undefined fields and must be empty after parsing.
	XXX map[string]interface{} `yaml:",inline"`

	// original is the input from which the config was parsed.
	original string
}

// LoadFile parses the given YAML file into a Config.
func LoadFile(filename string) (*Config, error) {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	cfg, err := Load(string(content))
	if err != nil {
		return nil, err
	}
	resolveFilepaths(filepath.Dir(filename), cfg)
	return cfg, nil
}
