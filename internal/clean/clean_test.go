package clean_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/dahai80/fusion-ops/internal/clean"
)

// writeTree builds a junk tree; mt is the mtime applied to every file.
func writeTree(t *testing.T, root string, files map[string]string, mt time.Time) {
	t.Helper()
	for rel, content := range files {
		full := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(full, mt, mt); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCleanDryRunDeletesNothing(t *testing.T) {
	root := t.TempDir()
	old := time.Now().Add(-10 * 24 * time.Hour)
	writeTree(t, root, map[string]string{
		"proj/target/debug/app":        "binary-bytes",
		"proj/pkg/__pycache__/mod.pyc": "pyc",
		"proj/keep.txt":                "keep",
		"logs/daemon.log":              "log-bytes",
	}, old)

	p := clean.DefaultPolicy()
	p.Roots = []string{root}
	p.DryRun = true
	var audited []clean.Action
	res := clean.Run(context.Background(), p, func(a clean.Action, decision string) {
		audited = append(audited, a)
	})

	if len(res.Deleted) != 3 {
		t.Fatalf("expected 3 dry-run candidates, got %d: %+v", len(res.Deleted), res.Deleted)
	}
	for _, f := range []string{"proj/target/debug/app", "proj/pkg/__pycache__/mod.pyc", "logs/daemon.log"} {
		if _, err := os.Stat(filepath.Join(root, f)); err != nil {
			t.Fatalf("dry-run must keep %s: %v", f, err)
		}
	}
	if len(audited) != 3 || audited[0].Path == "" {
		t.Fatalf("audit rows missing: %+v", audited)
	}
}

func TestCleanDeletesOldJunkRealRun(t *testing.T) {
	root := t.TempDir()
	old := time.Now().Add(-10 * 24 * time.Hour)
	fresh := time.Now()
	writeTree(t, root, map[string]string{
		"proj/target/debug/app": "stale-build",
		"proj/.venv/lib/x.so":   "venv-must-survive",
		"proj/keep.txt":         "keep",
		"logs/fresh.log":        "young",
	}, old)
	writeTree(t, root, map[string]string{"logs/fresh.log": "young"}, fresh)

	p := clean.DefaultPolicy()
	p.Roots = []string{root}
	p.DryRun = false
	var deleted, kept []string
	res := clean.Run(context.Background(), p, func(a clean.Action, decision string) {
		if decision == "deleted" {
			deleted = append(deleted, a.Path)
		} else if decision == "kept" {
			kept = append(kept, a.Path)
		}
	})

	if res.DeletedBytes != int64(len("stale-build")) {
		t.Fatalf("deleted bytes wrong: %d (%+v)", res.DeletedBytes, res.Deleted)
	}
	if _, err := os.Stat(filepath.Join(root, "proj/target/debug/app")); !os.IsNotExist(err) {
		t.Fatalf("stale build must be gone: %v", err)
	}
	for _, survivor := range []string{"proj/.venv/lib/x.so", "proj/keep.txt", "logs/fresh.log"} {
		if _, err := os.Stat(filepath.Join(root, survivor)); err != nil {
			t.Fatalf("must survive: %s (%v)", survivor, err)
		}
	}
	// Empty include-matched dirs pruned.
	if _, err := os.Stat(filepath.Join(root, "proj/target")); !os.IsNotExist(err) {
		t.Fatalf("empty target/ must be pruned: %v", err)
	}
	if len(deleted) != 1 || len(kept) != 0 {
		t.Fatalf("audit decisions wrong: deleted=%v kept=%v", deleted, kept)
	}
}

func TestCleanNeverDeletesGitTracked(t *testing.T) {
	root := t.TempDir()
	old := time.Now().Add(-30 * 24 * time.Hour)
	writeTree(t, root, map[string]string{"repo/dist/old.log": "tracked-log"}, old)
	if err := os.MkdirAll(filepath.Join(root, "repo"), 0o755); err != nil {
		t.Fatal(err)
	}
	gitInit := exec.Command("git", "-C", filepath.Join(root, "repo"), "init", "-q")
	if out, err := gitInit.CombinedOutput(); err != nil {
		t.Fatalf("git init: %v %s", err, out)
	}
	// dist/ tracked via a force-added file (contrived but proves the guard).
	add := exec.Command("git", "-C", filepath.Join(root, "repo"), "-c", "user.email=t@t", "-c", "user.name=t", "add", "-f", "dist/old.log")
	if out, err := add.CombinedOutput(); err != nil {
		t.Fatalf("git add: %v %s", err, out)
	}

	p := clean.DefaultPolicy()
	p.Roots = []string{root}
	p.DryRun = false
	var keptTracked int
	res := clean.Run(context.Background(), p, func(a clean.Action, decision string) {
		if decision == "kept" && a.Reason == "git-tracked" {
			keptTracked++
		}
	})

	if keptTracked != 1 {
		t.Fatalf("tracked file must be audited as kept: %d (%+v)", keptTracked, res)
	}
	if _, err := os.Stat(filepath.Join(root, "repo/dist/old.log")); err != nil {
		t.Fatalf("tracked file must survive: %v", err)
	}
}

func TestCleanBudgetCapStopsDeletion(t *testing.T) {
	root := t.TempDir()
	old := time.Now().Add(-10 * 24 * time.Hour)
	writeTree(t, root, map[string]string{
		"a/big.log": "0123456789",
		"b/big.log": "0123456789",
	}, old)

	p := clean.DefaultPolicy()
	p.Roots = []string{root}
	p.DryRun = false
	p.MaxDeleteMB = 1 // 1MB budget: both tiny files fit — set impossible cap
	p.MaxDeleteMB = 0 // 0 bytes: everything exceeds the budget
	res := clean.Run(context.Background(), p, func(clean.Action, string) {})

	if len(res.Deleted) != 0 || res.DeletedBytes != 0 {
		t.Fatalf("zero budget must delete nothing: %+v", res)
	}
	if len(res.Kept) != 2 {
		t.Fatalf("both files must be kept as budget-exceeded: %+v", res.Kept)
	}
	for _, f := range []string{"a/big.log", "b/big.log"} {
		if _, err := os.Stat(filepath.Join(root, f)); err != nil {
			t.Fatalf("budget guard failed for %s", f)
		}
	}
}

func TestLoadConfigDefaultsAndValidation(t *testing.T) {
	p, err := clean.LoadConfig(filepath.Join(t.TempDir(), "missing.json"))
	if err != nil || !p.DryRun || len(p.Include) == 0 {
		t.Fatalf("missing config must fall back to safe defaults: %+v err=%v", p, err)
	}
	bad := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(bad, []byte(`{"roots":[]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := clean.LoadConfig(bad); err == nil {
		t.Fatal("config without roots must error")
	}
}
