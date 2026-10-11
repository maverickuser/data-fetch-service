package api

import (
	"net/http/httptest"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/maverickuser/data-fetch-service/internal/config"
)

type openAPIDoc struct {
	Paths      map[string]map[string]any `yaml:"paths"`
	Components struct {
		Parameters map[string]struct {
			Schema struct {
				Enum []string `yaml:"enum"`
			} `yaml:"schema"`
		} `yaml:"parameters"`
	} `yaml:"components"`
}

func loadOpenAPI(t *testing.T) (openAPIDoc, map[string]any) {
	t.Helper()
	var doc openAPIDoc
	var raw map[string]any
	if err := yaml.Unmarshal(openAPIDocument, &doc); err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(openAPIDocument, &raw); err != nil {
		t.Fatal(err)
	}
	return doc, raw
}

func documentedRoutes(doc openAPIDoc) []string {
	var routes []string
	for path, item := range doc.Paths {
		for method := range item {
			if method == "parameters" {
				continue
			}
			routes = append(routes, strings.ToUpper(method)+" "+path)
		}
	}
	slices.Sort(routes)
	return routes
}

func TestOpenAPIMatchesRouterAndGateway(t *testing.T) {
	doc, _ := loadOpenAPI(t)
	documented := documentedRoutes(doc)

	var served []string
	for _, r := range (&Handler{}).routes() {
		served = append(served, r.pattern)
	}
	slices.Sort(served)
	if !slices.Equal(documented, served) {
		t.Fatalf("OpenAPI routes differ from the router:\n documented %v\n served     %v", documented, served)
	}

	tf, err := os.ReadFile("../../infra/service/api.tf")
	if err != nil {
		t.Fatal(err)
	}
	block := regexp.MustCompile(`(?s)api_routes = toset\(\[(.*?)\]\)`).FindSubmatch(tf)
	if block == nil {
		t.Fatal("api_routes not found in infra/service/api.tf")
	}
	var gateway []string
	for _, m := range regexp.MustCompile(`"([A-Z]+ /[^"]*)"`).FindAllSubmatch(block[1], -1) {
		gateway = append(gateway, string(m[1]))
	}
	slices.Sort(gateway)
	if !slices.Equal(documented, gateway) {
		t.Fatalf("OpenAPI routes differ from API Gateway:\n documented %v\n gateway    %v", documented, gateway)
	}
}

func TestOpenAPIEventTypesMatchConfiguration(t *testing.T) {
	doc, _ := loadOpenAPI(t)
	raw, err := os.ReadFile("../../config/events.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(raw)
	if err != nil {
		t.Fatal(err)
	}
	var configured []string
	for _, event := range cfg.Events {
		if event.Enabled {
			configured = append(configured, event.ID)
		}
	}
	documented := slices.Clone(doc.Components.Parameters["EventType"].Schema.Enum)
	slices.Sort(configured)
	slices.Sort(documented)
	if !slices.Equal(configured, documented) {
		t.Fatalf("EventType enum %v differs from enabled events %v", documented, configured)
	}
}

func TestOpenAPIReferencesResolve(t *testing.T) {
	_, raw := loadOpenAPI(t)
	var walk func(any)
	walk = func(node any) {
		switch v := node.(type) {
		case map[string]any:
			if ref, ok := v["$ref"].(string); ok {
				if !strings.HasPrefix(ref, "#/") {
					t.Errorf("non-local reference %s", ref)
					return
				}
				var target any = raw
				for _, part := range strings.Split(ref[2:], "/") {
					m, ok := target.(map[string]any)
					if !ok || m[part] == nil {
						t.Errorf("unresolved reference %s", ref)
						return
					}
					target = m[part]
				}
			}
			for _, child := range v {
				walk(child)
			}
		case []any:
			for _, child := range v {
				walk(child)
			}
		}
	}
	walk(raw)
	if raw["openapi"] != "3.1.0" {
		t.Fatalf("openapi version = %v", raw["openapi"])
	}
}

func TestDocsRoutesServeDocumentAndViewer(t *testing.T) {
	routes := (&Handler{}).Routes()

	rec := httptest.NewRecorder()
	routes.ServeHTTP(rec, httptest.NewRequest("GET", "/docs/openapi.yaml", nil))
	if rec.Code != 200 || rec.Header().Get("Content-Type") != "application/yaml" || rec.Body.String() != string(openAPIDocument) {
		t.Fatalf("openapi.yaml: %d %q", rec.Code, rec.Header().Get("Content-Type"))
	}

	rec = httptest.NewRecorder()
	routes.ServeHTTP(rec, httptest.NewRequest("GET", "/docs", nil))
	body := rec.Body.String()
	if rec.Code != 200 || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/html") {
		t.Fatalf("viewer: %d %q", rec.Code, rec.Header().Get("Content-Type"))
	}
	if !strings.Contains(body, `url: "/docs/openapi.yaml"`) || strings.Count(body, `integrity="sha384-`) != 2 {
		t.Fatal("viewer must load the served document with integrity-checked assets")
	}
	if !strings.Contains(body, "validatorUrl: null") {
		t.Fatal("viewer must not call the external validator, which its CSP blocks")
	}
	if csp := rec.Header().Get("Content-Security-Policy"); !strings.Contains(csp, "default-src 'none'") || !strings.Contains(csp, "connect-src 'self'") {
		t.Fatalf("viewer CSP = %q", csp)
	}

	rec = httptest.NewRecorder()
	routes.ServeHTTP(rec, httptest.NewRequest("POST", "/docs", nil))
	if rec.Code != 405 {
		t.Fatalf("POST /docs = %d", rec.Code)
	}
}
