package config

import (
	"gopkg.in/yaml.v2"

	"github.com/prometheus/promtheus/config"
)

// Config is the top-level configuration for Loki's config files.
type Config struct {
	// How frequently to scrape targets by default.
	ScrapeInterval time.Duration `yaml:"scrape_interval,omitempty"`
	// The default timeout when scraping targets.
	ScrapeTimeout time.Duration `yaml:"scrape_timeout,omitempty"`

	ScrapeConfigs []*config.ScrapeConfig `yaml:"scrape_configs,omitempty"`

	// original is the input from which the config was parsed.
	original string
}

var (
	// DefaultConfig is the default top-level configuration.
	DefaultConfig = Config{
		ScrapeInterval: 15 * time.Second,
		ScrapeTimeout:  10 * time.Second,
	}
)

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

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultConfig
	// We want to set c to the defaults and then overwrite it with the input.
	// To make unmarshal fill the plain data struct rather than calling UnmarshalYAML
	// again, we have to hide it using a type indirection.
	type plain Config
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}

	// Do global overrides and validate unique names.
	jobNames := map[string]struct{}{}
	for _, scfg := range c.ScrapeConfigs {
		// First set the correct scrape interval, then check that the timeout
		// (inferred or explicit) is not greater than that.
		if scfg.ScrapeInterval == 0 {
			scfg.ScrapeInterval = c.ScrapeInterval
		}
		if scfg.ScrapeTimeout > scfg.ScrapeInterval {
			return fmt.Errorf("scrape timeout greater than scrape interval for scrape config with job name %q", scfg.JobName)
		}
		if scfg.ScrapeTimeout == 0 {
			if c.ScrapeTimeout > scfg.ScrapeInterval {
				scfg.ScrapeTimeout = scfg.ScrapeInterval
			} else {
				scfg.ScrapeTimeout = c.ScrapeTimeout
			}
		}

		if _, ok := jobNames[scfg.JobName]; ok {
			return fmt.Errorf("found multiple scrape configs with job name %q", scfg.JobName)
		}
		jobNames[scfg.JobName] = struct{}{}
	}
	return nil
}
