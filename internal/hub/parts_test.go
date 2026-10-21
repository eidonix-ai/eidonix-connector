package hub

import (
	"testing"

	"github.com/eidonix-ai/eidonix-connector/protocol"
)

// A machine's parts merge across a site's sessions: a part is healthy when
// any session reports it healthy, and the service is ok, degraded or down.
func TestMergePartsAndHealth(t *testing.T) {
	qwen := protocol.ServicePart{Models: []string{"qwen"}, Endpoints: []string{"POST /v1/*"}}
	ocr := protocol.ServicePart{Models: []string{"ocr"}, Endpoints: []string{"POST /layout-parsing"}}
	one := []protocol.ServicePart{{Models: qwen.Models, Endpoints: qwen.Endpoints, Healthy: true}, {Models: ocr.Models, Endpoints: ocr.Endpoints}}
	two := []protocol.ServicePart{{Models: qwen.Models, Endpoints: qwen.Endpoints}, {Models: ocr.Models, Endpoints: ocr.Endpoints}}
	merged := mergeParts(mergeParts(nil, one), two)
	if len(merged) != 2 || !merged[0].Healthy || merged[1].Healthy {
		t.Fatalf("merged = %+v", merged)
	}
	if got := protocol.Health(true, merged); got != protocol.HealthDegraded {
		t.Fatalf("health = %s", got)
	}
	merged = mergeParts(merged, []protocol.ServicePart{{Models: ocr.Models, Endpoints: ocr.Endpoints, Healthy: true}})
	if got := protocol.Health(true, merged); got != protocol.HealthOK {
		t.Fatalf("health = %s", got)
	}
	if protocol.Health(false, nil) != protocol.HealthDown || protocol.Health(true, nil) != protocol.HealthOK {
		t.Fatal("a service without parts is ok or down")
	}
	if got := clipParts(make([]protocol.ServicePart, maxParts+5)); len(got) != maxParts {
		t.Fatalf("clipped to %d", len(got))
	}
}
