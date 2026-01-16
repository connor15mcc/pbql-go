package parser

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/bufbuild/protocompile"
	"github.com/bufbuild/protocompile/linker"
	"github.com/bufbuild/protocompile/reporter"
)

type Result struct {
	Files  []linker.File
	Errors []error
}

type Options struct {
	// ImportPaths specifies directories to search for imports.
	ImportPaths []string
	// Lenient continues parsing even when some files have errors.
	// Files with errors will be skipped but other files will still be processed.
	Lenient bool
}

// ParseFiles parses the given proto files and returns the compiled result.
func ParseFiles(ctx context.Context, files []string, opts Options) (*Result, error) {
	resolver := &protocompile.SourceResolver{
		ImportPaths: opts.ImportPaths,
	}

	warningW := os.Stderr
	var collectedErrors []error

	// Create a reporter that can be lenient about errors
	var rep reporter.Reporter
	if opts.Lenient {
		rep = reporter.NewReporter(
			func(err reporter.ErrorWithPos) error {
				collectedErrors = append(collectedErrors, err)
				// Return nil to continue processing
				return nil
			},
			func(err reporter.ErrorWithPos) {
				fmt.Fprintf(warningW, "warning: %v\n", err)
			},
		)
	} else {
		rep = reporter.NewReporter(
			func(err reporter.ErrorWithPos) error {
				return err // Return error to stop on first error
			},
			func(err reporter.ErrorWithPos) {
				fmt.Fprintf(warningW, "warning: %v\n", err)
			},
		)
	}

	compiler := &protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(resolver),
		Reporter: rep,
	}

	linked, err := compiler.Compile(ctx, files...)

	result := &Result{
		Files:  make([]linker.File, 0),
		Errors: collectedErrors,
	}

	// In lenient mode, we may get partial results even with errors
	for _, f := range linked {
		if f != nil {
			result.Files = append(result.Files, f)
		}
	}

	// If not lenient, return the error
	if !opts.Lenient && err != nil {
		return nil, err
	}

	// In lenient mode, only fail if we got zero files
	if opts.Lenient && len(result.Files) == 0 && err != nil {
		return nil, fmt.Errorf("no files could be parsed: %w", err)
	}

	return result, nil
}

func ParseDirectory(ctx context.Context, dir string, opts Options) (*Result, error) {
	var protoFiles []string

	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && filepath.Ext(path) == ".proto" {
			// Get path relative to dir for the compiler
			relPath, err := filepath.Rel(dir, path)
			if err != nil {
				return err
			}
			protoFiles = append(protoFiles, relPath)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	if len(protoFiles) == 0 {
		return &Result{}, nil
	}

	// Use the directory as an import path
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}

	// Change to the directory so relative paths work
	origDir, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	defer os.Chdir(origDir)

	if err := os.Chdir(absDir); err != nil {
		return nil, err
	}

	opts.ImportPaths = []string{"."}
	return ParseFiles(ctx, protoFiles, opts)
}
