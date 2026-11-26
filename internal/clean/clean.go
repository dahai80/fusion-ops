package clean

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
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
	MaxDeleteMB int `json:"max_delete_mb"`
	// QuarantineDir holds confirmed candidates for QuarantineDays before
	// real deletion (issue #17); empty defaults to a quarantine/ dir next
	// to the config (filled by the caller).
	QuarantineDir  string `json:"quarantine_dir"`
	QuarantineDays int    `json:"quarantine_days"`
	DryRun         bool   `json:"dry_run"`
}

// DefaultPolicy is conservative: known junk globs only, 7-day age floor,
// 2GB budget, dry-run on until an operator flips it. Model weights are
// always excluded (issue #17): trainer checkpoints live under the included
// runtime_cache/ tree, so a directory-name and extension guard is mandatory.
func DefaultPolicy() Policy {
	return Policy{
		Roots:   []string{"/opt/fusion-ops"},
		Include: defaultInclude(),
		Exclude: []string{
			".git/", ".venv/", "node_modules/", "*.sql.gz",
			"checkpoints/", "*.safetensors", "*.gguf", "*.ckpt", "*.pt", "*.pth",
		},
		MinAgeDays:     7,
		MaxDeleteMB:    2048,
		QuarantineDays: 7,
		DryRun:         true,
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
func protectedFiles(root string, excluded []string) map[string]bool {
	out := map[string]bool{}
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || !d.IsDir() {
			return nil
		}
		if d.Name() == ".git" {
			return fs.SkipDir
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr == nil && rel != "." {
			for _, ex := range excluded {
				// Repo discovery must not descend into heavy junk dirs
				// (.venv/node_modules hold no git repos but billions of
				// files — the first workspace dry-run timed out on them).
				if matchesGlob(rel, ex) {
					return fs.SkipDir
				}
			}
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

// Plan is the first-stage output of the two-stage confirmation flow
// (issue #17): the full candidate set plus a sha256 over it. Real
// deletion only happens in a second invocation whose re-scanned
// candidate set hashes to the same value.
type Plan struct {
	Hash        string    `json:"hash"`
	GeneratedAt time.Time `json:"generated_at"`
	Count       int       `json:"count"`
	TotalBytes  int64     `json:"total_bytes"`
	Candidates  []Action  `json:"candidates"`
}

// PlanRun sweeps every root against the policy without deleting anything.
// It audits kept rows and would_delete rows (reason depends on the mode:
// dry-run candidate vs pending confirmation) and returns the plan for the
// caller to persist and present for review.
func PlanRun(ctx context.Context, p Policy, audit func(Action, string)) (Plan, Result) {
	if !p.DryRun {
		PurgeQuarantine(ctx, p, audit)
	}
	res := Result{DryRun: p.DryRun}
	reason := "dry-run candidate"
	if !p.DryRun {
		reason = "pending confirmation"
	}
	candidates, res := scanRoots(ctx, p, audit, reason)

	modtimes := make(map[string]time.Time, len(candidates))
	for _, c := range candidates {
		if info, err := os.Stat(c.Path); err == nil {
			modtimes[c.Path] = info.ModTime()
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		ti, tj := modtimes[candidates[i].Path], modtimes[candidates[j].Path]
		return ti.Before(tj)
	})

	plan := Plan{
		GeneratedAt: time.Now().UTC(),
		Count:       len(candidates),
		Candidates:  candidates,
	}
	h := sha256.New()
	for _, c := range candidates {
		res.DeletedBytes += c.Bytes
		res.Deleted = append(res.Deleted, Action{Path: c.Path, Bytes: c.Bytes, Reason: reason})
		fmt.Fprintf(h, "%s\t%d\n", c.Path, c.Bytes)
	}
	plan.TotalBytes = res.DeletedBytes
	plan.Hash = fmt.Sprintf("%x", h.Sum(nil))
	return plan, res
}

// QuarantineManifest records one confirm batch: where each file came from
// and where it was parked, so expiry purges (and manual recovery) know the
// original path.
type QuarantineManifest struct {
	GeneratedAt time.Time        `json:"generated_at"`
	Entries     []QuarantineTerm `json:"entries"`
}

// QuarantineTerm is one manifest entry.
type QuarantineTerm struct {
	OriginalPath  string `json:"original_path"`
	QuarantinedAs string `json:"quarantined_as"`
	Bytes         int64  `json:"bytes"`
}

// ConfirmApply is the second stage (issue #17): the workspace is re-scanned
// and the candidate set hashed again. On a hash match candidates are moved
// into the quarantine dir (recoverable for QuarantineDays, purged by later
// runs), not deleted outright; any change (files added, removed, resized)
// invalidates the plan and the run refuses — re-plan and re-review.
func ConfirmApply(ctx context.Context, p Policy, audit func(Action, string), wantHash string) (Result, error) {
	if p.DryRun {
		return Result{}, fmt.Errorf("confirm apply requires dry_run=false in the policy")
	}
	if wantHash == "" {
		return Result{}, fmt.Errorf("confirm apply requires a reviewed plan hash")
	}
	PurgeQuarantine(ctx, p, audit)

	res := Result{DryRun: false}
	candidates, res := scanRoots(ctx, p, audit, "")

	h := sha256.New()
	for _, c := range candidates {
		fmt.Fprintf(h, "%s\t%d\n", c.Path, c.Bytes)
	}
	if got := fmt.Sprintf("%x", h.Sum(nil)); got != wantHash {
		return Result{}, fmt.Errorf("candidate set changed since plan (hash %s != %s): re-run to regenerate the plan", got, wantHash)
	}

	var batchDir string
	var manifest QuarantineManifest
	if p.QuarantineDir != "" {
		batchDir = filepath.Join(p.QuarantineDir, time.Now().UTC().Format("20060102T150405"))
		manifest = QuarantineManifest{GeneratedAt: time.Now().UTC()}
		if err := os.MkdirAll(batchDir, 0o700); err != nil {
			slog.Warn("quarantine dir unavailable; falling back to direct deletion", "dir", batchDir, "error", err)
			batchDir = ""
		}
	}

	budget := int64(p.MaxDeleteMB) << 20
	for _, c := range candidates {
		if res.DeletedBytes+c.Bytes > budget {
			res.Kept = append(res.Kept, Action{Path: c.Path, Bytes: c.Bytes, Reason: "budget exceeded"})
			audit(Action{Path: c.Path, Bytes: c.Bytes, Reason: "budget exceeded"}, "kept")
			continue
		}
		if batchDir != "" {
			dst := filepath.Join(batchDir, fmt.Sprintf("%06d_%s", len(manifest.Entries), filepath.Base(c.Path)))
			if rerr := os.Rename(c.Path, dst); rerr == nil {
				manifest.Entries = append(manifest.Entries, QuarantineTerm{OriginalPath: c.Path, QuarantinedAs: dst, Bytes: c.Bytes})
				res.DeletedBytes += c.Bytes
				res.Deleted = append(res.Deleted, Action{Path: c.Path, Bytes: c.Bytes, Reason: "quarantined"})
				audit(Action{Path: c.Path, Bytes: c.Bytes, Reason: "quarantined"}, "quarantined")
				continue
			} else {
				slog.Warn("quarantine rename failed; deleting in place", "path", c.Path, "error", rerr)
			}
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
	if len(manifest.Entries) > 0 {
		raw, err := json.MarshalIndent(manifest, "", "  ")
		if err == nil {
			if werr := os.WriteFile(filepath.Join(batchDir, "manifest.json"), raw, 0o600); werr != nil {
				slog.Warn("quarantine manifest write failed", "dir", batchDir, "error", werr)
			}
		} else {
			slog.Warn("quarantine manifest marshal failed", "error", err)
		}
	}
	for _, root := range p.Roots {
		removeEmptyDirs(root, p)
	}
	return res, nil
}

// PurgeQuarantine removes quarantine batches older than QuarantineDays —
// the final, already-twice-confirmed deletion. Called at the start of both
// plan and confirm runs (never in dry-run). Files whose manifest is
// unreadable are purged by batch dir mtime. Returns the purged file count.
func PurgeQuarantine(ctx context.Context, p Policy, audit func(Action, string)) int {
	if p.QuarantineDir == "" || p.DryRun {
		return 0
	}
	batches, err := os.ReadDir(p.QuarantineDir)
	if err != nil {
		return 0
	}
	expiry := time.Now().Add(-time.Duration(p.QuarantineDays) * 24 * time.Hour)
	purged := 0
	for _, b := range batches {
		if !b.IsDir() || ctx.Err() != nil {
			continue
		}
		batchDir := filepath.Join(p.QuarantineDir, b.Name())
		var manifest QuarantineManifest
		raw, rerr := os.ReadFile(filepath.Join(batchDir, "manifest.json"))
		if rerr == nil {
			_ = json.Unmarshal(raw, &manifest)
		}
		stale := manifest.GeneratedAt.Before(expiry)
		if !stale {
			if info, ierr := b.Info(); ierr == nil {
				stale = info.ModTime().Before(expiry)
			}
		}
		if !stale {
			continue
		}
		for _, e := range manifest.Entries {
			audit(Action{Path: e.OriginalPath, Bytes: e.Bytes, Reason: "quarantine expired (" + e.QuarantinedAs + ")"}, "deleted")
			purged++
		}
		if rerr != nil {
			slog.Warn("quarantine batch purged without readable manifest", "dir", batchDir, "error", rerr)
		}
		if rmErr := os.RemoveAll(batchDir); rmErr != nil {
			slog.Warn("quarantine purge failed", "dir", batchDir, "error", rmErr)
		}
	}
	return purged
}

// scanRoots walks every root and returns deletion candidates (include
// match + age floor + not git-tracked). Auditing: kept rows always
// audited; would_delete rows audited with wouldDeleteReason when set
// (empty during confirm re-scan — the plan already recorded them).
func scanRoots(ctx context.Context, p Policy, audit func(Action, string), wouldDeleteReason string) ([]Action, Result) {
	res := Result{DryRun: p.DryRun}
	var candidates []Action
	cutoff := time.Now().Add(-time.Duration(p.MinAgeDays) * 24 * time.Hour)

	for _, root := range p.Roots {
		protected := protectedFiles(root, p.Exclude)
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
	}
	if wouldDeleteReason != "" {
		for _, c := range candidates {
			audit(Action{Path: c.Path, Bytes: c.Bytes, Reason: wouldDeleteReason}, "would_delete")
		}
	}
	return candidates, res
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
