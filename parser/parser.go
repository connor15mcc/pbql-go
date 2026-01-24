package parser

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
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
}

// ParseFiles parses the given proto files and returns the compiled result.
func ParseFiles(ctx context.Context, files []string, opts Options) (*Result, error) {
	resolver := &protocompile.SourceResolver{
		ImportPaths: opts.ImportPaths,
	}

	var collectedErrors []error

	// Create a reporter that can be lenient about errors
	rep := reporter.NewReporter(
		func(err reporter.ErrorWithPos) error {
			collectedErrors = append(collectedErrors, err)
			slog.Warn(err.Error())
			// Return nil to continue processing
			return nil
		},
		func(err reporter.ErrorWithPos) {
			slog.Warn(err.Error())
		},
	)

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

	//  only fail if we got zero files
	if len(result.Files) == 0 && err != nil {
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
