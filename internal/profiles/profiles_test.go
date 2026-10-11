package profiles

import "testing"

func TestUnsafePaths(t *testing.T) {
	for _, p := range []string{"/v1/../admin", "/v1//chat", "/%2e%2e", "/%2f", "/a\\b", "/a\x00", "/a?b", "/a#b"} {
		if ValidatePath(p) == nil {
			t.Errorf("accepted %q", p)
		}
	}
}
func TestEveryFlavorDenylist(t *testing.T) {
	for name, f := range Flavors {
		for _, deny := range f.Denied {
			p := deny
			if p[len(p)-1] == '*' {
				p = p[:len(p)-1] + "evil"
			}
			kind := ""
			for _, k := range []string{"openai_chat", "openai_embeddings", "document_conversion"} {
				if kind == "" {
					for _, word := range split(f.Profiles) {
						if word == k {
							kind = k
						}
					}
				}
			}
			if _, e := Effective(kind, name, nil, []Pair{{"POST", p}}); e == nil {
				t.Errorf("%s allowed %s", name, p)
			}
		}
	}
}
func split(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ' ' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	return out
}
func TestResponsesNeedsOptIn(t *testing.T) {
	p, e := Effective("openai_chat", "", nil, nil)
	if e != nil {
		t.Fatal(e)
	}
	for _, v := range p {
		if v[1] == "/v1/responses" {
			t.Fatal("responses enabled by default")
		}
	}
}
