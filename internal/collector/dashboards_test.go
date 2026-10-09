package collector

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/davidcollom/komodor-metrics-exporter/internal/komodor"
)

// A renamed or removed metric would leave a dashboard panel silently empty, so every komodor_ name a
// dashboard queries must be one the exporter actually exposes.
func TestDashboardsOnlyQueryMetricsTheExporterExposes(t *testing.T) {
	closed := []komodor.Issue{{Type: "availability", Status: "closed", StartTime: 5, Summary: "x"}}
	srv := fakeAPI(&closed)
	defer srv.Close()
	reg := prometheus.NewRegistry()
	c := New(testClient(srv.URL, 4, 2, reg), time.Minute, time.Hour, allEnabled(t), IssueFilter{}, reg)
	c.poll(context.Background())

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	exposed := map[string]bool{}
	for _, mf := range mfs {
		name := mf.GetName()
		exposed[name] = true
		if mf.GetType() == dto.MetricType_HISTOGRAM {
			for _, s := range []string{"_bucket", "_sum", "_count"} {
				exposed[name+s] = true
			}
		}
	}

	files, err := filepath.Glob("../../dashboards/*.json")
	if err != nil || len(files) == 0 {
		t.Fatalf("no dashboards found: %v", err)
	}
	name := regexp.MustCompile(`komodor_[a-z_]*[a-z]`)
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var d struct {
			Panels []struct {
				Title   string `json:"title"`
				Targets []struct {
					Expr string `json:"expr"`
				} `json:"targets"`
			} `json:"panels"`
		}
		if err := json.Unmarshal(raw, &d); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		used := 0
		for _, p := range d.Panels {
			for _, tg := range p.Targets {
				for _, m := range name.FindAllString(tg.Expr, -1) {
					used++
					if !exposed[m] {
						t.Errorf("%s: panel %q queries %q, which the exporter does not expose", filepath.Base(f), p.Title, m)
					}
				}
			}
		}
		if used == 0 && !strings.Contains(f, "empty") {
			t.Errorf("%s: no komodor_ metric found in any panel; is the dashboard JSON shaped as expected?", filepath.Base(f))
		}
	}
}
