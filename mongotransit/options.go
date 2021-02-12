package mongotransit

import (
	"io/ioutil"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

type Collection string
type Database string

// Options contains CLI options
type Options struct {
	ReplicaSet     string             `yaml:"replicaSet"`
	ShardedCluster string             `yaml:"shardedCluster"`
	OutputDir      string             `yaml:"outputDir"`
	Collections    []CollectionOption `yaml:"collections"`
}

type CollectionOption struct {
	Name            Collection              `yaml:"name"`
	DB              Database                `yaml:"db"`
	UpsertFields    []string                `yaml:"upsertFields"`
	IterativeExport *IterativeExportOptions `yaml:"iterativeExport"`
}

type IterativeExportOptions struct {
	Field string `yaml:"field"`
	Force bool   `yanl:"force"`
}

// ReadConfigFile parses config yaml file
func ReadConfigFile(configPath string) (*Options, error) {
	configBytes, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, errors.Wrapf(err, "error opening file with --config")
	}

	// Unmarshal the config file as a top-level YAML file.
	var config Options
	err = yaml.UnmarshalStrict(configBytes, &config)
	if err != nil {
		return nil, errors.Wrapf(err, "error parsing config file %s", configPath)
	}

	return &config, nil
}
