package clean

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// Policy is the cleanup configuration (issue #15): what to sweep, how old
// files must be, and the per-run size budget. JSON on disk (see
// deploy/clean-config.json); every field has a safe default.
type Policy struct {
	Roots      []string `json:"roots"`
	Include    []string `json:"include"`
	Exclude    []string `json:"exclude"`
	MinAgeDays int      `json:"min_age_days"`
	// MaxDeleteMB is the per-run deletion budget; the sweep stops at the
	// cap so a bad glob can never exceed it (issue #15 req 3).
	MaxDeleteMB int  `json:"max_delete_mb"`
	DryRun      bool `json:"dry_run"`
}

// DefaultPolicy is conservative: known junk globs only, 7-day age floor,
// 2GB budget, dry-run on until an operator flips it.
func DefaultPolicy() Policy {
	return Policy{
		Roots:       []string{"/opt/fusion-ops"},
		Include:     defaultInclude(),
		Exclude:     []string{".git/", ".venv/", "node_modules/", "*.sql.gz"},
		MinAgeDays:  7,
		MaxDeleteMB: 2048,
		DryRun:      true,
	}
}

func defaultInclude() []string {
	return []string{
		"target/", "dist/", ".build/", ".bun-build/",
		"__pycache__/", ".ruff_cache/", "runtime_cache/",
		"coverage.out", "coverage.html", ".coverage",
		".DS_Store", "out/", "tmp/",
		"*.log", "*.log.*",
	}
}

// LoadConfig reads the policy JSON; missing file = DefaultPolicy.
func LoadConfig(path string) (Policy, error) {
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return DefaultPolicy(), nil
	}
	if err != nil {
		return Policy{}, fmt.Errorf("read clean config: %w", err)
	}
	var p Policy
	if err := json.Unmarshal(raw, &p); err != nil {
		return Policy{}, fmt.Errorf("parse clean config: %w", err)
	}
	if len(p.Roots) == 0 || len(p.Include) == 0 {
		return Policy{}, fmt.Errorf("clean config needs roots and include")
	}
	return p, nil
}

// Action is one audited decision (deleted, would_delete in dry-run, or
// kept because protected/oversized).
type Action struct {
	Path   string `json:"path"`
	Bytes  int64  `json:"bytes"`
	Reason string `json:"reason"`
}

// Result summarizes one sweep.
type Result struct {
	Deleted      []Action
	Kept         []Action // protected or budget-exceeded, kept with reason
	SkippedBytes int64
	DeletedBytes int64
	DryRun       bool
}

// protectedFiles collects git-tracked paths for one root (never delete
// tracked files, issue #15 req 6). Every nested git repo under the root
// contributes its tracked files (paths relative to the root).
func protectedFiles(root string) map[string]bool {
	out := map[string]bool{}
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || !d.IsDir() {
			return nil
		}
		if d.Name() == ".git" {
			return fs.SkipDir
		}
		if _, statErr := os.Stat(filepath.Join(path, ".git")); statErr != nil {
			return nil
		}
		relDir, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return nil
		}
		raw, outErr := exec.Command("git", "-C", path, "ls-files", "-z").Output()
		if outErr == nil {
			for _, f := range strings.Split(string(raw), "\x00") {
				if f != "" {
					out[filepath.Join(relDir, f)] = true
				}
			}
		}
		return fs.SkipDir // files enumerated; nested repos inside are their own roots
	})
	return out
}

// matchesGlob reports whether rel matches one include/exclude entry.
// Directory entries (trailing slash) match anywhere in the tree, at or
// below their name; file entries match basename or full rel path.
func matchesGlob(rel string, entry string) bool {
	if strings.HasSuffix(entry, "/") {
		return strings.Contains("/"+rel+"/", "/"+entry)
	}
	if ok, _ := filepath.Match(entry, filepath.Base(rel)); ok {
		return true
	}
	ok, _ := filepath.Match(entry, rel)
	return ok
}

// Run sweeps every root against the policy. Auditing is the caller's
// callback (DB insert in production, slice in tests). Deletion is
// oldest-first so the budget cap evicts the stalest junk first.
func Run(ctx context.Context, p Policy, audit func(Action, string)) Result {
	res := Result{DryRun: p.DryRun}
	cutoff := time.Now().Add(-time.Duration(p.MinAgeDays) * 24 * time.Hour)
	budget := int64(p.MaxDeleteMB) << 20

	for _, root := range p.Roots {
		protected := protectedFiles(root)
		var candidates []Action
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				slog.Warn("clean walk skip", "path", path, "error", err)
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			rel, relErr := filepath.Rel(root, path)
			if relErr != nil {
				return nil
			}
			if rel == "." {
				return nil
			}
			for _, ex := range p.Exclude {
				if matchesGlob(rel, ex) {
					if d.IsDir() {
						return fs.SkipDir
					}
					return nil
				}
			}
			if !matchesAny(rel, p.Include) {
				return nil
			}
			if d.IsDir() {
				// Directories are swept only when empty at the end; only
				// files carry size/age decisions.
				return nil
			}
			info, infoErr := d.Info()
			if infoErr != nil {
				return nil
			}
			if info.ModTime().After(cutoff) {
				res.SkippedBytes += info.Size()
				return nil
			}
			if protected[rel] {
				res.Kept = append(res.Kept, Action{Path: path, Bytes: info.Size(), Reason: "git-tracked"})
				audit(Action{Path: path, Bytes: info.Size(), Reason: "git-tracked"}, "kept")
				return nil
			}
			candidates = append(candidates, Action{Path: path, Bytes: info.Size()})
			return nil
		})
		if err != nil {
			slog.Error("clean walk aborted", "root", root, "error", err)
		}

		// Oldest first: stalest junk evicted within the budget.
		for i := 0; i < len(candidates); i++ {
			for j := i + 1; j < len(candidates); j++ {
				ii, _ := os.Stat(candidates[i].Path)
				jj, _ := os.Stat(candidates[j].Path)
				var ti, tj time.Time
				if ii != nil {
					ti = ii.ModTime()
				}
				if jj != nil {
					tj = jj.ModTime()
				}
				if tj.Before(ti) {
					candidates[i], candidates[j] = candidates[j], candidates[i]
				}
			}
		}
		for _, c := range candidates {
			if res.DeletedBytes+c.Bytes > budget {
				res.Kept = append(res.Kept, Action{Path: c.Path, Bytes: c.Bytes, Reason: "budget exceeded"})
				audit(Action{Path: c.Path, Bytes: c.Bytes, Reason: "budget exceeded"}, "kept")
				continue
			}
			if p.DryRun {
				res.DeletedBytes += c.Bytes
				res.Deleted = append(res.Deleted, Action{Path: c.Path, Bytes: c.Bytes, Reason: "dry-run candidate"})
				audit(Action{Path: c.Path, Bytes: c.Bytes, Reason: "dry-run candidate"}, "would_delete")
				continue
			}
			if err := os.Remove(c.Path); err != nil {
				slog.Warn("clean remove failed", "path", c.Path, "error", err)
				audit(Action{Path: c.Path, Bytes: c.Bytes, Reason: err.Error()}, "kept")
				continue
			}
			res.DeletedBytes += c.Bytes
			res.Deleted = append(res.Deleted, c)
			audit(c, "deleted")
		}
		removeEmptyDirs(root, p)
	}
	return res
}

func matchesAny(rel string, entries []string) bool {
	for _, in := range entries {
		if matchesGlob(rel, in) {
			return true
		}
	}
	return false
}

// removeEmptyDirs prunes directories matched by include globs after their
// contents are gone (build trees look clean again). Deepest first so a
// pruned leaf lets its parent go empty too.
func removeEmptyDirs(root string, p Policy) {
	var dirs []string
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err == nil && d.IsDir() {
			dirs = append(dirs, path)
		}
		return nil
	})
	for i := len(dirs) - 1; i >= 0; i-- {
		path := dirs[i]
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil || rel == "." {
			continue
		}
		if !matchesAny(rel, p.Include) {
			continue
		}
		skip := false
		for _, ex := range p.Exclude {
			if matchesGlob(rel, ex) {
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		if entries, e := os.ReadDir(path); e == nil && len(entries) == 0 {
			_ = os.Remove(path)
		}
	}
}
