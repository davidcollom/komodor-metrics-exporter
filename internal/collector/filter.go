package collector

import (
	"fmt"
	"slices"
	"strings"

	"github.com/davidcollom/komodor-metrics-exporter/internal/komodor"
)

// IssueFilter selects which cluster and issue-type pairs are queried. The issues API can fail for a
// single pair (a server-side 500), so a known-bad pair can be skipped instead of failing every poll.
type IssueFilter struct {
	types []string
	skips []skip
}

type skip struct{ raw, cluster, typ string }

// NewIssueFilter takes issue types to select (empty means all) and skip entries: a bare issue type
// (every cluster) or cluster/type, where either side may be "*".
func NewIssueFilter(types, skips []string) (IssueFilter, error) {
	f := IssueFilter{types: types}
	for _, t := range types {
		if !slices.Contains(komodor.IssueTypes, t) {
			return f, fmt.Errorf("unknown issue type %q, want one of %v", t, komodor.IssueTypes)
		}
	}
	for _, s := range skips {
		// A bare issue type is shorthand for every cluster, and avoids quoting "*/type" in YAML.
		if slices.Contains(komodor.IssueTypes, s) {
			f.skips = append(f.skips, skip{raw: s, cluster: "*", typ: s})
			continue
		}
		i := strings.LastIndex(s, "/")
		if i <= 0 || i == len(s)-1 {
			return f, fmt.Errorf("invalid skip-issues entry %q, want an issue type (%v) or cluster/type, where either may be *", s, komodor.IssueTypes)
		}
		sk := skip{raw: s, cluster: s[:i], typ: s[i+1:]}
		if sk.typ != "*" && !slices.Contains(komodor.IssueTypes, sk.typ) {
			return f, fmt.Errorf("invalid skip-issues entry %q: unknown issue type %q, want one of %v or *", s, sk.typ, komodor.IssueTypes)
		}
		f.skips = append(f.skips, sk)
	}
	return f, nil
}

func (s skip) matches(cluster, typ string) bool {
	return (s.cluster == "*" || s.cluster == cluster) && (s.typ == "*" || s.typ == typ)
}

func (f IssueFilter) Pairs(clusters []string) [][2]string {
	types := f.types
	if len(types) == 0 {
		types = komodor.IssueTypes
	}
	var out [][2]string
	for _, cl := range clusters {
		for _, t := range types {
			if !slices.ContainsFunc(f.skips, func(s skip) bool { return s.matches(cl, t) }) {
				out = append(out, [2]string{cl, t})
			}
		}
	}
	return out
}

// UnmatchedSkips returns entries naming a cluster that does not exist, which is almost always a typo.
func (f IssueFilter) UnmatchedSkips(clusters []string) []string {
	var out []string
	for _, s := range f.skips {
		if s.cluster != "*" && !slices.Contains(clusters, s.cluster) {
			out = append(out, s.raw)
		}
	}
	return out
}
