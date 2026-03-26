package conformance

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
)

type ExportFormat string

const (
	ExportFormatJSON  ExportFormat = "json"
	ExportFormatJSONL ExportFormat = "jsonl"
	ExportFormatCSV   ExportFormat = "csv"
	ExportFormatDOT   ExportFormat = "dot"
)

type Exporter interface {
	Export(dbPath string, format ExportFormat, outputPath string) ([]byte, error)
	Dump(dbPath string) ([]byte, error)
}

var testExporter Exporter = latticeExporter{}

type latticeExporter struct{}

var (
	latticeCLIOnce sync.Once
	latticeCLIPath string
	latticeCLIErr  error
)

func (latticeExporter) Export(dbPath string, format ExportFormat, outputPath string) ([]byte, error) {
	cliPath, err := ensureLatticeCLI()
	if err != nil {
		return nil, err
	}

	cmd := exec.Command(
		cliPath,
		"export",
		dbPath,
		"--file="+outputPath,
		"--format=json",
	)
	cmd.Dir = repoRoot()
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("export %s via lattice cli: %w\n%s", format, err, output)
	}
	return output, nil
}

func (latticeExporter) Dump(dbPath string) ([]byte, error) {
	cliPath, err := ensureLatticeCLI()
	if err != nil {
		return nil, err
	}

	cmd := exec.Command(cliPath, "dump", dbPath)
	cmd.Dir = repoRoot()

	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("dump via lattice cli: %w\n%s", err, stderr.Bytes())
	}
	return output, nil
}

func ensureLatticeCLI() (string, error) {
	latticeCLIOnce.Do(func() {
		root := repoRoot()
		cmd := exec.Command("zig", "build", "cli")
		cmd.Dir = root
		cmd.Env = append(
			os.Environ(),
			"ZIG_GLOBAL_CACHE_DIR=/tmp/zig-global-cache",
			fmt.Sprintf("ZIG_LOCAL_CACHE_DIR=%s", filepath.Join(root, "zig-cache")),
		)
		output, err := cmd.CombinedOutput()
		if err != nil {
			latticeCLIErr = fmt.Errorf("zig build cli: %w\n%s", err, output)
			return
		}
		latticeCLIPath = filepath.Join(root, "zig-out", "bin", "lattice")
		if _, err := os.Stat(latticeCLIPath); err != nil {
			latticeCLIErr = fmt.Errorf("locate built lattice cli at %s: %w", latticeCLIPath, err)
		}
	})
	return latticeCLIPath, latticeCLIErr
}

func repoRoot() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("cannot resolve conformance repo root")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}
