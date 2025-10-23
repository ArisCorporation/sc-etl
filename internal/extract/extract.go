package extract

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/ArisCorporation/sc-goetl/internal/config"
	"github.com/ArisCorporation/sc-goetl/internal/utils"
)

// Result captures the outcome of the extract phase.
type Result struct {
	RawDir        string
	NormalizedDir string
	Discovered    []string
}

var (
	requiredFiles = []string{"manufacturers.json", "ships.json", "items.json"}
	optionalFiles = []string{
		"ship_variants.json",
		"hardpoints.json",
		"item_stats.json",
		"ship_stats.json",
		"installed_items.json",
	}
)

// Run executes the extract phase of the ETL, invoking external tooling as configured.
func Run(ctx context.Context, cfg *config.Config) (*Result, error) {
	rawDir := filepath.Join(cfg.DataRoot, "raw", cfg.Channel, cfg.Version)
	normalizedDir := filepath.Join(cfg.DataRoot, "normalized", cfg.Channel, cfg.Version)

	rawDirHasData, err := utils.DirectoryHasPayload(rawDir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	shouldRunUnp4k := cfg.Unp4kEnable &&
		cfg.P4KPath != "" &&
		cfg.Unp4k.Bin != "" &&
		(cfg.ForceUnp4k || !rawDirHasData)

	if shouldRunUnp4k {
		if err := os.RemoveAll(rawDir); err != nil {
			return nil, fmt.Errorf("remove raw directory: %w", err)
		}
		if err := utils.EnsureDir(rawDir); err != nil {
			return nil, fmt.Errorf("ensure raw directory: %w", err)
		}

		argSets := [][]string{}
		if len(cfg.Unp4k.Args) > 0 {
			argSets = append(argSets, cfg.Unp4k.Args)
		} else {
			argSets = append(argSets,
				[]string{"{{p4k}}", "*.xml"},
				[]string{"{{p4k}}", "*.ini"},
			)
		}
		for _, args := range argSets {
			if err := runUnp4k(ctx, runUnp4kConfig{
				Bin:       cfg.Unp4k.Bin,
				Args:      args,
				P4KPath:   cfg.P4KPath,
				OutputDir: rawDir,
				WineBin:   cfg.WineBin,
			}); err != nil {
				return nil, err
			}
		}
	}

	if cfg.UnforgeEnable && cfg.Unforge.Bin != "" {
		if err := runUnforge(ctx, runUnforgeConfig{
			Bin:       cfg.Unforge.Bin,
			Args:      cfg.Unforge.Args,
			TargetDir: rawDir,
			WineBin:   cfg.WineBin,
		}); err != nil {
			return nil, err
		}
	}

	if cfg.ScdEnable && cfg.ScDataDump.Bin != "" {
		if err := runScDataDumper(ctx, runScDataDumperConfig{
			Bin:       cfg.ScDataDump.Bin,
			Args:      cfg.ScDataDump.Args,
			InputDir:  rawDir,
			OutputDir: coalesce(cfg.ScDataDump.OutputDir, rawDir),
			WineBin:   cfg.WineBin,
		}); err != nil {
			return nil, err
		}
	}

	exists, err := utils.PathExists(rawDir)
	if err != nil {
		return nil, err
	}
	if !exists {
		if cfg.P4KPath != "" {
			return nil, fmt.Errorf("raw data directory missing after extraction. expected files under %s", rawDir)
		}
		return nil, fmt.Errorf("raw data directory missing: %s", rawDir)
	}

	if err := validateRequiredFiles(rawDir); err != nil {
		return nil, err
	}

	discovered, err := discoverJSON(rawDir)
	if err != nil {
		return nil, err
	}

	if err := utils.EnsureDir(normalizedDir); err != nil {
		return nil, err
	}

	utils.Logger().Info("Extraction check complete",
		"raw_dir", rawDir,
		"normalized_dir", normalizedDir,
		"files", len(discovered),
	)

	return &Result{
		RawDir:        rawDir,
		NormalizedDir: normalizedDir,
		Discovered:    discovered,
	}, nil
}

type runUnp4kConfig struct {
	Bin       string
	Args      []string
	P4KPath   string
	OutputDir string
	WineBin   string
}

func runUnp4k(ctx context.Context, cfg runUnp4kConfig) error {
	bin := toAbs(cfg.Bin)
	templateArgs := cfg.Args
	if len(templateArgs) == 0 {
		templateArgs = []string{"{{p4k}}", "*.xml"}
	}

	absP4k := cfg.P4KPath
	if !filepath.IsAbs(absP4k) {
		if wd, err := os.Getwd(); err == nil {
			absP4k = filepath.Join(wd, absP4k)
		}
	}

	resolved := []string{}
	hasP4k := false
	for _, arg := range templateArgs {
		switch arg {
		case "{{p4k}}":
			resolved = append(resolved, absP4k)
			hasP4k = true
		case "{{output}}":
			resolved = append(resolved, cfg.OutputDir)
		default:
			value := strings.ReplaceAll(arg, "{{p4k}}", absP4k)
			value = strings.ReplaceAll(value, "{{output}}", cfg.OutputDir)
			if strings.Contains(value, absP4k) {
				hasP4k = true
			}
			resolved = append(resolved, value)
		}
	}
	if !hasP4k {
		resolved = append([]string{absP4k}, resolved...)
	}

	if err := utils.EnsureDir(cfg.OutputDir); err != nil {
		return err
	}

	utils.Logger().Info("Running unp4k", "bin", bin, "args", resolved, "p4k", absP4k, "output", cfg.OutputDir)
	return utils.RunExternal(ctx, bin, resolved, utils.RunOptions{
		WorkingDir: cfg.OutputDir,
		WineBin:    cfg.WineBin,
	})
}

type runUnforgeConfig struct {
	Bin       string
	Args      []string
	TargetDir string
	WineBin   string
}

func runUnforge(ctx context.Context, cfg runUnforgeConfig) error {
	bin := toAbs(cfg.Bin)
	templateArgs := cfg.Args
	if len(templateArgs) == 0 {
		templateArgs = []string{"{{input}}"}
	}

	absTarget := cfg.TargetDir
	if !filepath.IsAbs(absTarget) {
		if wd, err := os.Getwd(); err == nil {
			absTarget = filepath.Join(wd, absTarget)
		}
	}

	resolved := []string{}
	hasInput := false
	for _, arg := range templateArgs {
		switch arg {
		case "{{input}}":
			resolved = append(resolved, absTarget)
			hasInput = true
		default:
			value := strings.ReplaceAll(arg, "{{input}}", absTarget)
			if strings.Contains(value, absTarget) {
				hasInput = true
			}
			resolved = append(resolved, value)
		}
	}
	if !hasInput {
		return errors.New("unforge arguments must include the {{input}} placeholder")
	}

	utils.Logger().Info("Running unforge", "bin", bin, "args", resolved, "target", absTarget)
	return utils.RunExternal(ctx, bin, resolved, utils.RunOptions{
		WorkingDir: absTarget,
		WineBin:    cfg.WineBin,
	})
}

type runScDataDumperConfig struct {
	Bin       string
	Args      []string
	InputDir  string
	OutputDir string
	WineBin   string
}

func runScDataDumper(ctx context.Context, cfg runScDataDumperConfig) error {
	if len(cfg.Args) == 0 {
		utils.Logger().Warn("scdatadumper configured without arguments – skipping converter step.")
		return nil
	}

	bin := toAbs(cfg.Bin)
	absInput := toAbs(cfg.InputDir)
	absOutput := toAbs(cfg.OutputDir)

	argPlaceholders := make([]*string, len(cfg.Args))
	resolved := make([]string, len(cfg.Args))
	for i, arg := range cfg.Args {
		switch arg {
		case "{{input}}":
			resolved[i] = absInput
			argPlaceholders[i] = &cfg.InputDir
		case "{{output}}":
			resolved[i] = absOutput
			argPlaceholders[i] = &cfg.OutputDir
		default:
			value := strings.ReplaceAll(arg, "{{input}}", absInput)
			value = strings.ReplaceAll(value, "{{output}}", absOutput)
			resolved[i] = value
		}
	}

	normalizedBin := strings.ToLower(filepath.Base(bin))
	hostBaseDir := toAbs("bins/scdatadumper")
	hostImportDir := filepath.Join(hostBaseDir, "import")
	hostExportDir := filepath.Join(hostBaseDir, "export")
	cliEntrypoint := filepath.Join(hostBaseDir, "cli.php")

	if err := os.RemoveAll(hostImportDir); err != nil {
		return err
	}
	if err := utils.CopyDir(absInput, hostImportDir); err != nil {
		return err
	}
	if err := os.Chmod(hostImportDir, 0o777); err != nil {
		return err
	}

	if err := os.RemoveAll(hostExportDir); err != nil {
		return err
	}
	if err := utils.EnsureDir(hostExportDir); err != nil {
		return err
	}
	if err := os.Chmod(hostExportDir, 0o777); err != nil {
		return err
	}

	containerInput := "/var/www/html/import"
	containerOutput := "/var/www/html/export"
	isComposeWrapper := normalizedBin == "docker-compose" || normalizedBin == "podman-compose"
	isComposeCLI := normalizedBin == "docker" || normalizedBin == "podman"

	if isComposeCLI {
		if len(resolved) == 0 || resolved[0] != "compose" {
			resolved = append([]string{"compose"}, resolved...)
		}
	}
	if isComposeCLI || isComposeWrapper {
		hasFileFlag := false
		for _, arg := range resolved {
			if arg == "-f" || arg == "--file" {
				hasFileFlag = true
				break
			}
		}
		if !hasFileFlag {
			if isComposeCLI {
				resolved = append([]string{"compose", "-f", filepath.Join(hostBaseDir, "compose.yaml")}, resolved[1:]...)
			} else {
				resolved = append([]string{"-f", filepath.Join(hostBaseDir, "compose.yaml")}, resolved...)
			}
		}
		for i, placeholder := range argPlaceholders {
			if placeholder == nil {
				continue
			}
			switch *placeholder {
			case cfg.InputDir:
				resolved[i] = containerInput
			case cfg.OutputDir:
				resolved[i] = containerOutput
			}
		}
		for i, value := range resolved {
			value = strings.ReplaceAll(value, absInput, containerInput)
			value = strings.ReplaceAll(value, absOutput, containerOutput)
			resolved[i] = value
		}
	}

	utils.Logger().Info("Running scdatadumper", "bin", bin, "args", resolved, "input", cfg.InputDir, "output", cfg.OutputDir)

	if isComposeCLI || isComposeWrapper {
		upArgs := []string{"up", "-d"}
		baseArgs := []string{}
		if isComposeCLI {
			baseArgs = append([]string{"compose", "-f", filepath.Join(hostBaseDir, "compose.yaml")}, upArgs...)
		} else {
			baseArgs = append([]string{"-f", filepath.Join(hostBaseDir, "compose.yaml")}, upArgs...)
		}
		if err := utils.RunExternal(ctx, bin, baseArgs, utils.RunOptions{WineBin: cfg.WineBin}); err != nil {
			return err
		}
		execPrefix := []string{}
		if isComposeCLI {
			execPrefix = []string{"compose", "-f", filepath.Join(hostBaseDir, "compose.yaml"), "exec", "scdatadumper"}
		} else {
			execPrefix = []string{"-f", filepath.Join(hostBaseDir, "compose.yaml"), "exec", "scdatadumper"}
		}
		generateArgs := append(execPrefix, "php", "cli.php", "generate:cache", containerInput)
		if err := utils.RunExternal(ctx, bin, generateArgs, utils.RunOptions{WineBin: cfg.WineBin}); err != nil {
			return err
		}
		if err := utils.RunExternal(ctx, bin, resolved, utils.RunOptions{WineBin: cfg.WineBin}); err != nil {
			return err
		}
	} else {
		if normalizedBin == "php" || strings.HasSuffix(normalizedBin, ".php") {
			resolveArgs := func(args []string) []string {
				out := make([]string, len(args))
				for i, arg := range args {
					switch arg {
					case "cli.php":
						out[i] = cliEntrypoint
					case cfg.InputDir, absInput:
						out[i] = absInput
					case cfg.OutputDir, absOutput:
						out[i] = absOutput
					default:
						value := strings.ReplaceAll(arg, cfg.InputDir, absInput)
						value = strings.ReplaceAll(value, cfg.OutputDir, absOutput)
						out[i] = value
					}
				}
				return out
			}
			if err := utils.RunExternal(ctx, bin, resolveArgs([]string{"cli.php", "generate:cache", absInput}), utils.RunOptions{
				WineBin:    cfg.WineBin,
				WorkingDir: hostBaseDir,
			}); err != nil {
				return err
			}
			if err := utils.RunExternal(ctx, bin, resolveArgs(resolved), utils.RunOptions{
				WineBin:    cfg.WineBin,
				WorkingDir: hostBaseDir,
			}); err != nil {
				return err
			}
		} else {
			if err := utils.RunExternal(ctx, bin, resolved, utils.RunOptions{WineBin: cfg.WineBin}); err != nil {
				return err
			}
		}
	}

	if exists, err := utils.PathExists(hostExportDir); err == nil && exists {
		if err := utils.EnsureDir(absOutput); err != nil {
			return err
		}
		if err := utils.CopyDir(hostExportDir, absOutput); err != nil {
			return err
		}
	}
	return nil
}

func coalesce(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func toAbs(path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	if path == "" {
		return ""
	}
	if wd, err := os.Getwd(); err == nil {
		return filepath.Join(wd, path)
	}
	return filepath.Clean(path)
}

func validateRequiredFiles(rawDir string) error {
	for _, file := range requiredFiles {
		exists, err := utils.PathExists(filepath.Join(rawDir, file))
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("missing required raw data file: %s", file)
		}
	}
	var missing []string
	for _, file := range optionalFiles {
		exists, err := utils.PathExists(filepath.Join(rawDir, file))
		if err != nil {
			return err
		}
		if !exists {
			missing = append(missing, file)
		}
	}
	if len(missing) > 0 {
		utils.Logger().Warn("Optional raw files missing (will be skipped)", "files", missing)
	}
	return nil
}

func discoverJSON(root string) ([]string, error) {
	discovered := []string{}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(strings.ToLower(d.Name()), ".json") {
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			discovered = append(discovered, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(discovered)
	return discovered, nil
}

func init() {
	// Windows path normalisation ensures consistent logging.
	if runtime.GOOS == "windows" {
		for i, file := range requiredFiles {
			requiredFiles[i] = filepath.ToSlash(file)
		}
		for i, file := range optionalFiles {
			optionalFiles[i] = filepath.ToSlash(file)
		}
	}
}
