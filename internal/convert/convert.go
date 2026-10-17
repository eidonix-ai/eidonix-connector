// Package convert turns an older connector configuration into a v0.3
// connector.yaml, listing anything it could not carry for the owner to review.
// A v0.2 YAML file is converted directly; a v0.1 cletus-connector TOML file
// (and the secret files it references) is first converted to v0.2.
package convert

import (
	"bytes"
	"fmt"
	"os"

	"github.com/eidonix-ai/eidonix-connector/internal/config"
	"github.com/eidonix-ai/eidonix-connector/protocol"
	"gopkg.in/yaml.v3"
)

// Convert reads the file at path and returns the v0.3 YAML and the review
// notes. site is the connector's name in Eidonix Setup.
func Convert(path, site string) ([]byte, []string, error) {
	if !protocol.ValidSiteName(site) {
		return nil, nil, fmt.Errorf("--site %q must be the connector's name in Eidonix Setup: 1-40 lowercase letters, digits or -", site)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	var notes []string
	switch detect(data) {
	case "v0.3":
		return nil, nil, fmt.Errorf("%s is already a v0.3 configuration (it has services:); check it with eidonix-connector check --config %s", path, path)
	case "v0.1":
		v02, v1notes, err := fromV01(path, data)
		if err != nil {
			return nil, nil, err
		}
		data, notes = v02, v1notes
	}
	out, v2notes, err := fromV02(data, site)
	if err != nil {
		return nil, nil, fmt.Errorf("%s: %w", path, err)
	}
	notes = append(notes, v2notes...)
	if _, err := config.Parse(out); err != nil {
		notes = append(notes, fmt.Sprintf("the converted file does not pass check yet: %v", err))
	}
	return out, notes, nil
}

// detect tells a v0.3 or v0.2 YAML file from a v0.1 TOML file.
func detect(data []byte) string {
	var doc yaml.Node
	if yaml.NewDecoder(bytes.NewReader(data)).Decode(&doc) == nil && len(doc.Content) == 1 && doc.Content[0].Kind == yaml.MappingNode {
		root := doc.Content[0]
		for i := 0; i+1 < len(root.Content); i += 2 {
			if root.Content[i].Value == "services" || root.Content[i].Value == "site" {
				return "v0.3"
			}
		}
		for i := 0; i+1 < len(root.Content); i += 2 {
			if root.Content[i].Value == "upstream" {
				return "v0.2"
			}
		}
	}
	return "v0.1"
}
