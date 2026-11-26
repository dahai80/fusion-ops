package clean_test

import (
	"context"
	"encoding/json"
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
	_, res := clean.PlanRun(context.Background(), p, func(a clean.Action, decision string) {
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

func TestCleanTwoStageConfirmDeletes(t *testing.T) {
	root := t.TempDir()
	old := time.Now().Add(-10 * 24 * time.Hour)
	fresh := time.Now()
	writeTree(t, root, map[string]string{
		"proj/target/debug/app": "stale-build",
		"proj/.venv/lib/x.so":   "venv-must-survive",
		"proj/keep.txt":         "keep",
	}, old)
	writeTree(t, root, map[string]string{"logs/fresh.log": "young"}, fresh)

	p := clean.DefaultPolicy()
	p.Roots = []string{root}
	p.DryRun = false
	p.QuarantineDir = filepath.Join(root, "..", "quarantine-test")
	var quarantined, kept []string
	audit := func(a clean.Action, decision string) {
		switch decision {
		case "quarantined":
			quarantined = append(quarantined, a.Path)
		case "kept":
			kept = append(kept, a.Path)
		}
	}

	// Stage 1: plan only — nothing deleted, empty dirs not pruned.
	plan, res := clean.PlanRun(context.Background(), p, audit)
	if len(plan.Candidates) != 1 || plan.Hash == "" || plan.TotalBytes != int64(len("stale-build")) {
		t.Fatalf("plan wrong: %+v", plan)
	}
	if _, err := os.Stat(filepath.Join(root, "proj/target/debug/app")); err != nil {
		t.Fatalf("plan phase must not delete: %v", err)
	}
	if len(quarantined) != 0 || len(res.Deleted) != 1 {
		t.Fatalf("plan phase audited deletion: quarantined=%v res=%+v", quarantined, res)
	}

	// Stage 2: confirm with the reviewed hash — candidate moves to quarantine.
	res2, err := clean.ConfirmApply(context.Background(), p, audit, plan.Hash)
	if err != nil {
		t.Fatalf("confirm apply: %v", err)
	}
	if res2.DeletedBytes != int64(len("stale-build")) || len(quarantined) != 1 {
		t.Fatalf("confirm quarantined wrong set: %+v (%+v)", res2, quarantined)
	}
	if _, err := os.Stat(filepath.Join(root, "proj/target/debug/app")); !os.IsNotExist(err) {
		t.Fatalf("stale build must be gone from the workspace: %v", err)
	}
	// Quarantine holds the file + a restorable manifest.
	qb, rerr := os.ReadDir(p.QuarantineDir)
	if rerr != nil || len(qb) != 1 {
		t.Fatalf("quarantine batch missing: %v (%+v)", rerr, qb)
	}
	mraw, merr := os.ReadFile(filepath.Join(p.QuarantineDir, qb[0].Name(), "manifest.json"))
	if merr != nil {
		t.Fatalf("manifest missing: %v", merr)
	}
	var man clean.QuarantineManifest
	if err := json.Unmarshal(mraw, &man); err != nil || len(man.Entries) != 1 || man.Entries[0].OriginalPath != filepath.Join(root, "proj/target/debug/app") {
		t.Fatalf("manifest wrong: %v (%.200s)", err, mraw)
	}
	if _, err := os.Stat(man.Entries[0].QuarantinedAs); err != nil {
		t.Fatalf("quarantined file missing: %v", err)
	}
	for _, survivor := range []string{"proj/.venv/lib/x.so", "proj/keep.txt", "logs/fresh.log"} {
		if _, err := os.Stat(filepath.Join(root, survivor)); err != nil {
			t.Fatalf("must survive: %s (%v)", survivor, err)
		}
	}
	// Empty include-matched dirs pruned only at confirm.
	if _, err := os.Stat(filepath.Join(root, "proj/target")); !os.IsNotExist(err) {
		t.Fatalf("empty target/ must be pruned: %v", err)
	}
	if len(kept) != 0 {
		t.Fatalf("unexpected kept rows: %v", kept)
	}
}

func TestCleanConfirmHashMismatchRefuses(t *testing.T) {
	root := t.TempDir()
	old := time.Now().Add(-10 * 24 * time.Hour)
	writeTree(t, root, map[string]string{"a/old.log": "stale"}, old)

	p := clean.DefaultPolicy()
	p.Roots = []string{root}
	p.DryRun = false
	plan, _ := clean.PlanRun(context.Background(), p, func(clean.Action, string) {})

	// Workspace changed after review: a new candidate appears.
	writeTree(t, root, map[string]string{"b/newer.log": "also stale"}, old.Add(-time.Hour))

	_, err := clean.ConfirmApply(context.Background(), p, func(clean.Action, string) {}, plan.Hash)
	if err == nil {
		t.Fatal("hash mismatch must refuse deletion")
	}
	if _, serr := os.Stat(filepath.Join(root, "a/old.log")); serr != nil {
		t.Fatalf("refused run must not delete: %v", serr)
	}
}

func TestCleanQuarantinePurgeExpired(t *testing.T) {
	root := t.TempDir()
	old := time.Now().Add(-10 * 24 * time.Hour)
	writeTree(t, root, map[string]string{"a/old.log": "stale"}, old)

	p := clean.DefaultPolicy()
	p.Roots = []string{root}
	p.DryRun = false
	p.QuarantineDir = filepath.Join(root, "..", "quarantine-purge")
	p.QuarantineDays = 7

	// Pre-existing expired batch (as if confirmed 8+ days ago).
	batch := filepath.Join(p.QuarantineDir, "20261101T000000")
	if err := os.MkdirAll(batch, 0o700); err != nil {
		t.Fatal(err)
	}
	parked := filepath.Join(batch, "000000_old.log")
	if err := os.WriteFile(parked, []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	man := clean.QuarantineManifest{
		GeneratedAt: time.Now().Add(-8 * 24 * time.Hour),
		Entries:     []clean.QuarantineTerm{{OriginalPath: "/somewhere/old.log", QuarantinedAs: parked, Bytes: 5}},
	}
	raw, _ := json.Marshal(man)
	if err := os.WriteFile(filepath.Join(batch, "manifest.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}

	var purgeDeleted []string
	audit := func(a clean.Action, decision string) {
		if decision == "deleted" {
			purgeDeleted = append(purgeDeleted, a.Path)
		}
	}
	// Plan phase (real, not dry-run) purges expired batches.
	candidates := clean.PurgeQuarantine(context.Background(), p, audit)
	if candidates != 1 {
		t.Fatalf("expected 1 purged file, got %d", candidates)
	}
	if _, err := os.Stat(batch); !os.IsNotExist(err) {
		t.Fatalf("expired batch must be removed: %v", err)
	}
	if len(purgeDeleted) != 1 || purgeDeleted[0] != "/somewhere/old.log" {
		t.Fatalf("purge audit rows wrong: %v", purgeDeleted)
	}

	// Fresh batch survives.
	writeTree(t, root, map[string]string{"b/new.log": "stale"}, old)
	plan, _ := clean.PlanRun(context.Background(), p, audit)
	if _, err := clean.ConfirmApply(context.Background(), p, audit, plan.Hash); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	batches, _ := os.ReadDir(p.QuarantineDir)
	if len(batches) != 1 {
		t.Fatalf("fresh batch must survive purge: %+v", batches)
	}
}

func TestCleanModelWeightsNeverCandidates(t *testing.T) {
	root := t.TempDir()
	old := time.Now().Add(-30 * 24 * time.Hour)
	writeTree(t, root, map[string]string{
		"trainer/runtime_cache/checkpoints/sft_7b/model.safetensors": "weights",
		"trainer/runtime_cache/checkpoints/sft_7b/ckpt.pt":           "weights",
		"models/llama.gguf":                                           "weights",
		"runs/ckpt/step-100.ckpt":                                     "weights",
		"junk/old.log":                                                "stale-log",
	}, old)

	p := clean.DefaultPolicy()
	p.Roots = []string{root}
	p.DryRun = false
	plan, _ := clean.PlanRun(context.Background(), p, func(clean.Action, string) {})

	if len(plan.Candidates) != 1 || filepath.Base(plan.Candidates[0].Path) != "old.log" {
		t.Fatalf("weights must never be candidates: %+v", plan.Candidates)
	}
	for _, w := range []string{
		"trainer/runtime_cache/checkpoints/sft_7b/model.safetensors",
		"trainer/runtime_cache/checkpoints/sft_7b/ckpt.pt",
		"models/llama.gguf",
		"runs/ckpt/step-100.ckpt",
	} {
		if _, err := os.Stat(filepath.Join(root, w)); err != nil {
			t.Fatalf("weight file must survive: %s (%v)", w, err)
		}
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
	_, res := clean.PlanRun(context.Background(), p, func(a clean.Action, decision string) {
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
	p.MaxDeleteMB = 0 // 0 bytes: everything exceeds the budget
	plan, _ := clean.PlanRun(context.Background(), p, func(clean.Action, string) {})
	var kept []string
	res, err := clean.ConfirmApply(context.Background(), p, func(a clean.Action, decision string) {
		if decision == "kept" {
			kept = append(kept, a.Path)
		}
	}, plan.Hash)
	if err != nil {
		t.Fatalf("confirm apply: %v", err)
	}

	if len(res.Deleted) != 0 || res.DeletedBytes != 0 {
		t.Fatalf("zero budget must delete nothing: %+v", res)
	}
	if len(kept) != 2 {
		t.Fatalf("both files must be kept as budget-exceeded: %v", kept)
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
	found := false
	for _, ex := range p.Exclude {
		if ex == "checkpoints/" || ex == "*.safetensors" {
			found = true
		}
	}
	if !found {
		t.Fatalf("default excludes must guard model weights: %v", p.Exclude)
	}
	bad := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(bad, []byte(`{"roots":[]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := clean.LoadConfig(bad); err == nil {
		t.Fatal("config without roots must error")
	}
}
