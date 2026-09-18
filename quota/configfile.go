package quota

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// fileConfig 是 JSON 配置文件的可演进结构。
//
// 版本演进规则：仅新增带 omitempty 的字段；未知字段默认拒绝解析，
// 避免旧实例误用新语义（调用方可通过 Decoder 自行放宽）。
type fileConfig struct {
	Version             string     `json:"version"`
	DefaultWeight       int        `json:"defaultWeight"`
	Degrade             string     `json:"degrade"`
	GlobalMaxConcurrent int64      `json:"globalMaxConcurrent,omitempty"`
	Rules               []fileRule `json:"rules"`
}

type fileRule struct {
	Tenant        string  `json:"tenant"`
	API           string  `json:"api,omitempty"`
	Rate          float64 `json:"rate"`
	Burst         int64   `json:"burst"`
	MaxConcurrent int64   `json:"maxConcurrent"`
	Weight        int     `json:"weight,omitempty"`
}

// ParseConfig 从 JSON 解析并校验配置。校验失败时返回错误，配置不会被采用。
func ParseConfig(r io.Reader) (*Config, error) {
	raw, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	var fc fileConfig
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&fc); err != nil {
		return nil, newConfigError("", fmt.Sprintf("parse json: %v", err))
	}
	if dec.More() {
		return nil, newConfigError("", "multiple JSON documents are not allowed")
	}
	dp, err := ParseDegradePolicy(fc.Degrade)
	if err != nil {
		return nil, newConfigError("degrade", err.Error())
	}
	cfg := &Config{
		Version:             fc.Version,
		DefaultWeight:       fc.DefaultWeight,
		Degrade:             dp,
		GlobalMaxConcurrent: fc.GlobalMaxConcurrent,
		Rules:               make([]Rule, 0, len(fc.Rules)),
	}
	for _, fr := range fc.Rules {
		cfg.Rules = append(cfg.Rules, Rule{
			Tenant:        fr.Tenant,
			API:           fr.API,
			Rate:          fr.Rate,
			Burst:         fr.Burst,
			MaxConcurrent: fr.MaxConcurrent,
			Weight:        fr.Weight,
		})
	}
	if err := Validate(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// LoadConfigFile 从文件路径读取配置（便于热更新时重新加载）。
func LoadConfigFile(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return ParseConfig(f)
}
