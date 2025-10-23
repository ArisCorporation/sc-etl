package utils

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
)

// EnsureDir creates a directory path if it does not exist.
func EnsureDir(path string) error {
	return os.MkdirAll(path, 0o775)
}

// PathExists returns true when the filesystem path exists.
func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

// ReadJSON loads JSON from disk into the provided destination.
func ReadJSON(path string, dst any) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	return decoder.Decode(dst)
}

// ReadJSONBytes returns the raw JSON bytes for custom unmarshalling paths.
func ReadJSONBytes(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return io.ReadAll(file)
}

// ReadJSONOrDefault loads JSON when the file exists; otherwise returns fallback.
func ReadJSONOrDefault(path string, fallback any, dst any) error {
	exists, err := PathExists(path)
	if err != nil {
		return err
	}
	if !exists {
		if dst != nil && fallback != nil {
			bytes, err := json.Marshal(fallback)
			if err != nil {
				return err
			}
			return json.Unmarshal(bytes, dst)
		}
		return nil
	}
	return ReadJSON(path, dst)
}

// WriteJSON writes JSON to disk after ensuring the directory exists.
func WriteJSON(path string, data any) error {
	dir := filepath.Dir(path)
	if err := EnsureDir(dir); err != nil {
		return err
	}
	tempFile, err := os.CreateTemp(dir, "tmpjson-*")
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(tempFile)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(data); err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())
		return err
	}
	if err := tempFile.Sync(); err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())
		return err
	}
	if err := tempFile.Close(); err != nil {
		os.Remove(tempFile.Name())
		return err
	}
	return os.Rename(tempFile.Name(), path)
}

// ReadJSONGeneric returns a JSON object from disk.
func ReadJSONGeneric(path string) (map[string]any, error) {
	bytes, err := ReadJSONBytes(path)
	if err != nil {
		return nil, err
	}
	var result map[string]any
	if err := json.Unmarshal(bytes, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// ReadJSONArrayGeneric returns a JSON array from disk.
func ReadJSONArrayGeneric(path string) ([]any, error) {
	bytes, err := ReadJSONBytes(path)
	if err != nil {
		return nil, err
	}
	var result []any
	if err := json.Unmarshal(bytes, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// CopyDir recursively copies src into dest, replacing existing files.
func CopyDir(src, dest string) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("source is not a directory")
	}
	if err := EnsureDir(dest); err != nil {
		return err
	}
	return filepath.WalkDir(src, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dest, rel)
		if d.IsDir() {
			if err := EnsureDir(target); err != nil {
				return err
			}
			return os.Chmod(target, 0o775)
		}
		if !d.Type().IsRegular() {
			return nil
		}
		return copyFile(path, target)
	})
}

func copyFile(src, dest string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	if err := EnsureDir(filepath.Dir(dest)); err != nil {
		return err
	}
	out, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Chmod(0o664)
}
