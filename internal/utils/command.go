package utils

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// RunOptions controls the execution of external commands.
type RunOptions struct {
	WorkingDir string
	WineBin    string
}

// RunExternal executes an external command, optionally wrapping Windows binaries with Wine.
func RunExternal(ctx context.Context, bin string, args []string, opts RunOptions) error {
	if ctx == nil {
		ctx = context.Background()
	}

	spawnBin := bin
	spawnArgs := append([]string(nil), args...)

	isPathLike := strings.ContainsRune(bin, '/') || strings.ContainsRune(bin, '\\')
	if isPathLike && !filepath.IsAbs(bin) {
		base := opts.WorkingDir
		if base == "" {
			if wd, err := os.Getwd(); err == nil {
				base = wd
			}
		}
		if base != "" {
			spawnBin = filepath.Clean(filepath.Join(base, bin))
		}
	}

	if needsWine(spawnBin) {
		wine := opts.WineBin
		if wine == "" {
			wine = "wine"
		}
		spawnArgs = append([]string{spawnBin}, spawnArgs...)
		spawnBin = wine
	}

	cmd := exec.CommandContext(ctx, spawnBin, spawnArgs...)
	if opts.WorkingDir != "" {
		cmd.Dir = opts.WorkingDir
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin

	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return fmt.Errorf("%s exited with code %d", spawnBin, exitErr.ExitCode())
		}
		return err
	}
	return nil
}

func needsWine(bin string) bool {
	if runtime.GOOS == "windows" {
		return false
	}
	return strings.HasSuffix(strings.ToLower(bin), ".exe")
}

// DirectoryHasPayload checks whether a directory exists and contains entries other than .gitkeep.
func DirectoryHasPayload(path string) (bool, error) {
	exists, err := PathExists(path)
	if err != nil || !exists {
		return false, err
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if entry.Name() != ".gitkeep" {
			return true, nil
		}
	}
	return false, nil
}
