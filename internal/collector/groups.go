package collector

import "fmt"

// Each name is both a config toggle and a collection step label.
const (
	MetricClusters     = "clusters"
	MetricRisks        = "risks"
	MetricRisksActive  = "risks_active"
	MetricRisksByCheck = "risks_by_check"
	MetricIssues       = "issues"
)

var MetricNames = []string{MetricClusters, MetricRisks, MetricRisksActive, MetricRisksByCheck, MetricIssues}

// ParseEnabled starts with everything on, applies explicit config values, then the disabled list.
func ParseEnabled(cfg map[string]bool, disabled []string) (map[string]bool, error) {
	out := map[string]bool{}
	for _, n := range MetricNames {
		out[n] = true
	}
	set := func(name string, on bool) error {
		if _, ok := out[name]; !ok {
			return fmt.Errorf("unknown metric group %q, want one of %v", name, MetricNames)
		}
		out[name] = on
		return nil
	}
	for n, on := range cfg {
		if err := set(n, on); err != nil {
			return nil, err
		}
	}
	for _, n := range disabled {
		if err := set(n, false); err != nil {
			return nil, err
		}
	}
	return out, nil
}
