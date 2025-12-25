package config

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
)

const (
	defaultDataRoot        = "./data"
	defaultP4KPath         = "./data/p4k/Data.p4k"
	defaultScDataDumperBin = "php"
)

var defaultScDataDumperArgs = []string{
	"-d", "memory_limit=2G",
	"cli.php", "load:data",
	"--scUnpackedFormat", "{{input}}", "{{output}}",
}

// Config collects runtime configuration for the ETL pipeline.
type Config struct {
	Channel                string
	Version                string
	DataRoot               string
	P4KPath                string
	LoadEnabled            bool
	PromoteVersions        bool
	DirectusURL            string
	DirectusToken          string
	DefaultCompanyCategory string
	ForceUnp4k             bool
	Unp4k                  ExternalToolConfig
	Unforge                ExternalToolConfig
	ScDataDump             ExternalToolConfig
	Unp4kEnable            bool
	UnforgeEnable          bool
	ScdEnable              bool
	WineBin                string
}

// ExternalToolConfig describes a command line integration for native tooling.
type ExternalToolConfig struct {
	Bin       string
	Args      []string
	OutputDir string
}

type stringSlice []string

func (s *stringSlice) String() string {
	return strings.Join(*s, ",")
}

func (s *stringSlice) Set(value string) error {
	*s = append(*s, value)
	return nil
}

// Parse validates CLI flags and environment variables into a Config value.
func Parse(args []string) (*Config, error) {
	cfg := &Config{}
	fs := flag.NewFlagSet("scgoetl", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	defaultChannel := envString("CHANNEL", "LIVE")
	fs.StringVar(&cfg.Channel, "channel", defaultChannel, "Target channel (LIVE, PTU, EPTU)")

	defaultVersion := envString("GAME_VERSION", "0.0.0")
	fs.StringVar(&cfg.Version, "version", defaultVersion, "Target game version")

	defaultRoot := envString("DATA_ROOT", defaultDataRoot)
	fs.StringVar(&cfg.DataRoot, "data-root", defaultRoot, "Root directory for ETL assets")

	fs.StringVar(&cfg.P4KPath, "p4k", envStringList([]string{"P4K_PATH", "DATA_P4K"}, defaultP4KPath), "Path to Data.p4k archive")

	cfg.LoadEnabled = envBool("LOAD_ENABLED", true)
	fs.BoolVar(&cfg.LoadEnabled, "load-enabled", cfg.LoadEnabled, "Enable Directus load step")

	cfg.DirectusURL = envString("DIRECTUS_URL", "")
	fs.StringVar(&cfg.DirectusURL, "directus-url", cfg.DirectusURL, "Directus base URL")

	cfg.DirectusToken = envString("DIRECTUS_TOKEN", "")
	fs.StringVar(&cfg.DirectusToken, "directus-token", cfg.DirectusToken, "Directus static token")

	cfg.PromoteVersions = envBool("PROMOTE_VERSIONS", true)
	fs.BoolVar(&cfg.PromoteVersions, "promote-versions", cfg.PromoteVersions, "Promote Directus content versions (disable to keep main unchanged)")

	cfg.DefaultCompanyCategory = envString("DEFAULT_COMPANY_CATEGORY", "")
	fs.StringVar(&cfg.DefaultCompanyCategory, "default-company-category", cfg.DefaultCompanyCategory, "Fallback Directus company category id when none can be detected")

	cfg.ForceUnp4k = envBool("FORCE_UNP4K", false)
	fs.BoolVar(&cfg.ForceUnp4k, "force-unp4k", cfg.ForceUnp4k, "Force re-extraction with unp4k")

	cfg.Unp4kEnable = envBool("UNP4K_ENABLED", cfg.P4KPath != "")
	fs.BoolVar(&cfg.Unp4kEnable, "unp4k-enabled", cfg.Unp4kEnable, "Enable unp4k extraction step")

	cfg.Unp4k.Bin = envString("UNP4K_BIN", "bins/unp4k/unp4k.exe")
	fs.StringVar(&cfg.Unp4k.Bin, "unp4k-bin", cfg.Unp4k.Bin, "unp4k executable path")

	var unp4kArgs stringSlice
	if envArgs := splitEnvArgs("UNP4K_ARGS"); len(envArgs) > 0 {
		unp4kArgs = append(unp4kArgs, envArgs...)
	}
	fs.Var(&unp4kArgs, "unp4k-arg", "Additional argument for unp4k (repeatable)")

	cfg.UnforgeEnable = envBool("UNFORGE_ENABLED", cfg.Unp4kEnable)
	fs.BoolVar(&cfg.UnforgeEnable, "unforge-enabled", cfg.UnforgeEnable, "Enable unforge conversion step")

	cfg.Unforge.Bin = envString("UNFORGE_BIN", "bins/unp4k/unforge.exe")
	fs.StringVar(&cfg.Unforge.Bin, "unforge-bin", cfg.Unforge.Bin, "unforge executable path")

	var unforgeArgs stringSlice
	if envArgs := splitEnvArgs("UNFORGE_ARGS"); len(envArgs) > 0 {
		unforgeArgs = append(unforgeArgs, envArgs...)
	}
	fs.Var(&unforgeArgs, "unforge-arg", "Additional argument for unforge (repeatable)")

	cfg.ScdEnable = envBool("SC_DATA_DUMPER_ENABLED", true)
	fs.BoolVar(&cfg.ScdEnable, "scd-enabled", cfg.ScdEnable, "Enable StarCitizenDataDumper integration")

	cfg.ScDataDump.Bin = envString("SC_DATA_DUMPER_BIN", defaultScDataDumperBin)
	fs.StringVar(&cfg.ScDataDump.Bin, "scd-bin", cfg.ScDataDump.Bin, "scDataDumper executable wrapper path")

	var scdArgs stringSlice
	if envArgs := splitEnvArgs("SC_DATA_DUMPER_ARGS"); len(envArgs) > 0 {
		scdArgs = append(scdArgs, envArgs...)
	}
	fs.Var(&scdArgs, "scd-arg", "Additional argument for scDataDumper (repeatable)")

	cfg.ScDataDump.OutputDir = envString("SC_DATA_DUMPER_OUTPUT", "")
	fs.StringVar(&cfg.ScDataDump.OutputDir, "scd-output", cfg.ScDataDump.OutputDir, "Output directory for scDataDumper results")

	cfg.WineBin = envString("WINE_BIN", defaultWine())
	fs.StringVar(&cfg.WineBin, "wine-bin", cfg.WineBin, "Wine binary to wrap Windows tools on non-Windows hosts")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	if cfg.Channel == "" {
		return nil, errors.New("channel must be specified")
	}

	cfg.Unp4k.Args = append(cfg.Unp4k.Args, unp4kArgs...)
	cfg.Unforge.Args = append(cfg.Unforge.Args, unforgeArgs...)
	cfg.ScDataDump.Args = append(cfg.ScDataDump.Args, scdArgs...)
	if len(cfg.ScDataDump.Args) == 0 {
		cfg.ScDataDump.Args = append(cfg.ScDataDump.Args, defaultScDataDumperArgs...)
	}

	cfg.DirectusURL = strings.TrimSpace(cfg.DirectusURL)
	cfg.DirectusToken = strings.TrimSpace(cfg.DirectusToken)
	cfg.DefaultCompanyCategory = strings.TrimSpace(cfg.DefaultCompanyCategory)

	if cfg.ScdEnable && cfg.ScDataDump.Bin == "" {
		return nil, fmt.Errorf("scDataDumper enabled but no --scd-bin provided")
	}

	return cfg, nil
}

func envString(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		return value
	}
	return fallback
}

func envStringList(keys []string, fallback string) string {
	for _, key := range keys {
		if value, ok := os.LookupEnv(key); ok && value != "" {
			return value
		}
	}
	return fallback
}

func splitEnvArgs(key string) []string {
	value, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(value) == "" {
		return nil
	}
	fields := strings.Fields(value)
	return append([]string(nil), fields...)
}

func envBool(key string, fallback bool) bool {
	if value, ok := os.LookupEnv(key); ok {
		if parsed, err := strconv.ParseBool(value); err == nil {
			return parsed
		}
	}
	return fallback
}

func defaultWine() string {
	if isWindows() {
		return ""
	}
	return "wine"
}

func isWindows() bool {
	return runtime.GOOS == "windows"
}
