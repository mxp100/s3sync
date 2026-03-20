package main

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Credentials struct {
	AccessKeyID     string `yaml:"accessKeyId"`
	SecretAccessKey string `yaml:"secretAccessKey"`
}

type Location struct {
	Bucket          string       `yaml:"bucket"`
	Prefix          string       `yaml:"prefix"`
	Region          string       `yaml:"region"`
	Host            string       `yaml:"host"`
	PathStyle       bool         `yaml:"pathStyle"`
	AllowSelfSigned bool         `yaml:"allowSelfSigned,omitempty"`
	Credentials     *Credentials `yaml:"credentials,omitempty"`
}

type Options struct {
	DeleteExtra          bool `yaml:"deleteExtra"`
	OverwriteOnNameMatch bool `yaml:"overwriteOnNameMatch"`
	CopyWorkers          int  `yaml:"copyWorkers,omitempty"`
}

type SyncJob struct {
	Name        string   `yaml:"name"`
	Source      Location `yaml:"source"`
	Destination Location `yaml:"destination"`
	Options     Options  `yaml:"options"`
}

func LoadConfig(path string) ([]SyncJob, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var jobs []SyncJob
	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	if err := dec.Decode(&jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}
