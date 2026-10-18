package architecture

import (
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const repositoryRoot = "../.."

// allowedImports lists, for every production package, the module packages it may import. Together the entries
// form the layering, lowest first:
//
//	domain, config                 foundation: no module imports
//	events                         trigger normalization
//	state, acquisition             S3 coordination; source downloads and validation
//	telemetry, storage             outcome records; S3 artifacts
//	pull                           complete-event coordination
//	admission, delivery            admission; processor handoff
//	api, recovery                  REST adapters; reconciler
//	runtime                        wiring of every adapter, imported only by cmd/
//	cmd/*                          thin entry points
//
// queue is a leaf adapter wired by runtime. smoke and awsverify are verification code that production
// packages never import. A new edge is a design change: add it here and explain it in the PR.
var allowedImports = map[string][]string{
	"cmd/admission":         {"internal/runtime"},
	"cmd/api":               {"internal/runtime"},
	"cmd/configcheck":       {"internal/config", "internal/events"},
	"cmd/delivery":          {"internal/runtime"},
	"cmd/pull":              {"internal/runtime"},
	"cmd/reconciler":        {"internal/runtime"},
	"cmd/smoke":             {"internal/delivery", "internal/smoke"},
	"internal/acquisition":  {"internal/config"},
	"internal/admission":    {"internal/acquisition", "internal/config", "internal/domain", "internal/events", "internal/pull", "internal/state", "internal/telemetry"},
	"internal/api":          {"internal/admission", "internal/config", "internal/domain", "internal/events", "internal/state", "internal/telemetry"},
	"internal/architecture": {},
	"internal/awsverify":    {},
	"internal/config":       {},
	"internal/delivery":     {"internal/domain", "internal/events", "internal/pull", "internal/state", "internal/telemetry"},
	"internal/domain":       {},
	"internal/events":       {"internal/config", "internal/domain"},
	"internal/pull":         {"internal/acquisition", "internal/config", "internal/domain", "internal/events", "internal/state", "internal/telemetry"},
	"internal/queue":        {},
	"internal/recovery":     {"internal/delivery", "internal/domain", "internal/events", "internal/state", "internal/telemetry"},
	"internal/runtime":      {"internal/acquisition", "internal/admission", "internal/api", "internal/config", "internal/delivery", "internal/events", "internal/pull", "internal/queue", "internal/recovery", "internal/state", "internal/storage", "internal/telemetry"},
	"internal/smoke":        {},
	"internal/state":        {"internal/domain", "internal/events"},
	"internal/storage":      {"internal/acquisition", "internal/state"},
	"internal/telemetry":    {"internal/state"},
}

// awsFreePackages hold business rules and must stay testable without any AWS type.
var awsFreePackages = []string{"internal/domain", "internal/config", "internal/events", "internal/acquisition", "internal/telemetry"}

// restrictedImports are external packages that only the listed module packages may import.
var restrictedImports = []struct {
	path      string
	importers []string
	reason    string
}{
	{"github.com/aws/aws-lambda-go/lambda", []string{"internal/runtime"},
		"only runtime starts Lambda handlers; cmd/ calls runtime, and other packages take plain events"},
	{"github.com/aws/aws-sdk-go-v2/config", []string{"internal/runtime", "cmd/smoke"},
		"AWS configuration and credentials are loaded once at the edge and passed in as clients"},
}

// TestPackageImportsFollowTheLayering fails when a production package breaks the allowed import graph.
func TestPackageImportsFollowTheLayering(t *testing.T) {
	module := modulePath(t)
	graph := importGraph(t)
	for _, problem := range violations(module, graph) {
		t.Error(problem)
	}
}

// TestViolationsExplainTheFix checks that each rule reports a breach with a message that says what to do.
func TestViolationsExplainTheFix(t *testing.T) {
	const module = "example.com/m"
	graph := map[string][]string{
		"internal/acquisition": {module + "/internal/pull"},
		"internal/domain":      {"github.com/aws/aws-sdk-go-v2/aws"},
		"internal/api":         {"github.com/aws/aws-lambda-go/lambda"},
		"internal/newpackage":  {},
	}
	want := []string{
		"internal/acquisition imports internal/pull",
		"Move the shared code into a lower package",
		"internal/domain imports github.com/aws/aws-sdk-go-v2/aws",
		"internal/api imports github.com/aws/aws-lambda-go/lambda; only runtime starts Lambda handlers",
		"internal/newpackage is not in allowedImports",
		"allowedImports lists internal/queue, which has no production Go files",
	}
	got := strings.Join(violations(module, graph), "\n")
	for _, fragment := range want {
		if !strings.Contains(got, fragment) {
			t.Errorf("violations missing %q in:\n%s", fragment, got)
		}
	}
}

// violations returns every breach of the layering rules in graph, which maps a repository-relative package
// directory to the import paths of its production files. Messages say how to fix the breach.
func violations(module string, graph map[string][]string) []string {
	var problems []string
	for _, pkg := range sortedKeys(graph) {
		allowed, known := allowedImports[pkg]
		if !known {
			problems = append(problems, fmt.Sprintf("%s is not in allowedImports. Add it in internal/architecture/architecture_test.go "+
				"with the module packages it imports, and add it to the AGENTS.md repository map.", pkg))
		}
		for _, imported := range graph[pkg] {
			if local, ok := strings.CutPrefix(imported, module+"/"); ok {
				if known && !slices.Contains(allowed, local) {
					problems = append(problems, fmt.Sprintf("%s imports %s, which its layer may not use (allowed: %s). "+
						"Move the shared code into a lower package both can import, or, if this is a deliberate design change, "+
						"add the edge to allowedImports and explain it in the PR.", pkg, local, describe(allowed)))
				}
				continue
			}
			if slices.Contains(awsFreePackages, pkg) && strings.HasPrefix(imported, "github.com/aws/") {
				problems = append(problems, fmt.Sprintf("%s imports %s, but it must stay free of AWS types. "+
					"Define a small interface or plain value type here and implement it in an adapter package (state, storage, queue, delivery).", pkg, imported))
			}
			for _, rule := range restrictedImports {
				if (imported == rule.path || strings.HasPrefix(imported, rule.path+"/")) && !slices.Contains(rule.importers, pkg) {
					problems = append(problems, fmt.Sprintf("%s imports %s; %s. Only %s may import it.", pkg, imported, rule.reason, describe(rule.importers)))
				}
			}
		}
	}
	for _, pkg := range sortedKeys(allowedImports) {
		if _, present := graph[pkg]; !present {
			problems = append(problems, fmt.Sprintf("allowedImports lists %s, which has no production Go files. Remove the entry.", pkg))
		}
	}
	return problems
}

// importGraph parses every non-test Go file under cmd/ and internal/, whatever its build tags, and returns the
// imports of each package directory.
func importGraph(t *testing.T) map[string][]string {
	t.Helper()
	graph := map[string][]string{}
	for _, top := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(repositoryRoot, top), func(path string, entry os.DirEntry, err error) error {
			if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				return err
			}
			relative, err := filepath.Rel(repositoryRoot, filepath.Dir(path))
			if err != nil {
				return err
			}
			pkg := filepath.ToSlash(relative)
			imports := graph[pkg]
			for _, spec := range file.Imports {
				imported, err := strconv.Unquote(spec.Path.Value)
				if err != nil {
					return err
				}
				if !slices.Contains(imports, imported) {
					imports = append(imports, imported)
				}
			}
			graph[pkg] = imports
			return nil
		})
		if err != nil {
			t.Fatalf("reading %s: %v", top, err)
		}
	}
	return graph
}

// modulePath returns the module path declared in go.mod.
func modulePath(t *testing.T) string {
	t.Helper()
	content, err := os.ReadFile(filepath.Join(repositoryRoot, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(content), "\n") {
		if module, ok := strings.CutPrefix(strings.TrimSpace(line), "module "); ok {
			return strings.TrimSpace(module)
		}
	}
	t.Fatal("go.mod declares no module")
	return ""
}

// describe joins package names for a message, or says none are allowed.
func describe(packages []string) string {
	if len(packages) == 0 {
		return "no module packages"
	}
	return strings.Join(packages, ", ")
}

// sortedKeys returns the keys of a map in ascending order.
func sortedKeys(values map[string][]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
