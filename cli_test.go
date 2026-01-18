package main

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
)

func captureOutput(f func() error) (string, string, error) {
	oldStdout := os.Stdout
	oldStderr := os.Stderr

	rOut, wOut, _ := os.Pipe()
	rErr, wErr, _ := os.Pipe()

	os.Stdout = wOut
	os.Stderr = wErr

	err := f()

	wOut.Close()
	wErr.Close()
	os.Stdout = oldStdout
	os.Stderr = oldStderr

	outBuf := new(bytes.Buffer)
	errBuf := new(bytes.Buffer)
	io.Copy(outBuf, rOut)
	io.Copy(errBuf, rErr)

	return outBuf.String(), errBuf.String(), err
}

func TestHelpFlag(t *testing.T) {
	stdout, stderr, err := captureOutput(func() error {
		return mainE([]string{"--help"})
	})
	_ = stdout
	_ = stderr
	if err != nil {
		t.Errorf("Expected no error for --help, got: %v", err)
	}
}

func TestHelpFlagCapture(t *testing.T) {
	stdout, _, err := captureOutput(func() error {
		return mainE([]string{"--help"})
	})

	if err != nil {
		t.Errorf("Expected no error for --help, got: %v", err)
	}

	expectedContents := []string{
		"Query protobuf definitions using SQL",
		"-q, --query string",
		"-f, --format string",
		"-v, --verbose",
		"Count methods per service",
	}

	for _, expected := range expectedContents {
		if !strings.Contains(stdout, expected) {
			t.Errorf("Expected help output to contain %q, but stdout was: %s", expected, stdout)
		}
	}
}

func TestNoArgsError(t *testing.T) {
	stdout, stderr, err := captureOutput(func() error {
		return mainE([]string{})
	})
	_ = stdout
	_ = stderr

	if err == nil {
		t.Error("Expected error for no arguments")
	}

	expected := "at least one proto file or directory is required"
	if !strings.Contains(err.Error(), expected) {
		t.Errorf("Expected error containing %q, got %q", expected, err.Error())
	}
}

func TestInvalidFlag(t *testing.T) {
	stdout, stderr, err := captureOutput(func() error {
		return mainE([]string{"--invalid-flag"})
	})
	_ = stdout
	_ = stderr

	if err == nil {
		t.Error("Expected error for invalid flag")
	}

	if !strings.Contains(err.Error(), "unknown flag") {
		t.Errorf("Expected error about unknown flag, got: %v", err)
	}
}

func TestFormatFlagTable(t *testing.T) {
	stdout, stderr, err := captureOutput(func() error {
		return mainE([]string{"-q", "SELECT 1 as test", "testdata"})
	})
	_ = stdout
	_ = stderr

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestFormatFlagJSON(t *testing.T) {
	stdout, stderr, err := captureOutput(func() error {
		return mainE([]string{"-q", "SELECT 1 as test", "-f", "json", "testdata"})
	})
	_ = stderr

	if err != nil {
		t.Errorf("Unexpected error with JSON format: %v", err)
	}

	if !strings.HasPrefix(strings.TrimSpace(stdout), "[") {
		t.Errorf("Expected JSON output to start with [, got: %s", stdout)
	}
}

func TestFormatFlagCSV(t *testing.T) {
	stdout, stderr, err := captureOutput(func() error {
		return mainE([]string{"-q", "SELECT 1 as test", "-f", "csv", "testdata"})
	})
	_ = stderr

	if err != nil {
		t.Errorf("Unexpected error with CSV format: %v", err)
	}

	if !strings.Contains(stdout, "test") {
		t.Errorf("Expected CSV output to contain 'test', got: %s", stdout)
	}
}

func TestQueryFlag(t *testing.T) {
	stdout, stderr, err := captureOutput(func() error {
		return mainE([]string{"--query", "SELECT name FROM sqlite_master WHERE type='table'", "testdata"})
	})
	_ = stderr

	if err != nil {
		t.Errorf("Unexpected error with query flag: %v", err)
	}

	expectedTables := []string{"files", "messages", "fields"}
	for _, table := range expectedTables {
		if !strings.Contains(stdout, table) {
			t.Errorf("Expected output to contain table %q, got: %s", table, stdout)
		}
	}
}

func TestVerboseFlagLevel0(t *testing.T) {
	stdout, stderr, err := captureOutput(func() error {
		return mainE([]string{"-q", "SELECT 1", "testdata"})
	})
	_ = stdout
	_ = stderr

	if err != nil {
		t.Errorf("Unexpected error with verbose flag: %v", err)
	}
}

func TestVerboseFlagLevel1(t *testing.T) {
	stdout, stderr, err := captureOutput(func() error {
		return mainE([]string{"-v", "-q", "SELECT 1", "testdata"})
	})
	_ = stdout
	_ = stderr

	if err != nil {
		t.Errorf("Unexpected error with -v flag: %v", err)
	}
}

func TestVerboseFlagLevel2(t *testing.T) {
	stdout, stderr, err := captureOutput(func() error {
		return mainE([]string{"-vv", "-q", "SELECT 1", "testdata"})
	})
	_ = stdout
	_ = stderr

	if err != nil {
		t.Errorf("Unexpected error with -vv flag: %v", err)
	}
}

func TestVerboseFlagLongForm(t *testing.T) {
	stdout, stderr, err := captureOutput(func() error {
		return mainE([]string{"--verbose", "-q", "SELECT 1", "testdata"})
	})
	_ = stdout
	_ = stderr

	if err != nil {
		t.Errorf("Unexpected error with --verbose flag: %v", err)
	}
}

func TestFlagCombinations(t *testing.T) {
	stdout, stderr, err := captureOutput(func() error {
		return mainE([]string{
			"-q", "SELECT name FROM sqlite_master WHERE type='table'",
			"-f", "json",
			"-v",
			"testdata",
		})
	})
	_ = stderr

	if err != nil {
		t.Errorf("Unexpected error with flag combination: %v", err)
	}

	if !strings.HasPrefix(strings.TrimSpace(stdout), "[") {
		t.Errorf("Expected JSON output, got: %s", stdout)
	}
}
