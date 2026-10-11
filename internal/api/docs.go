package api

import (
	_ "embed"
	"net/http"
)

// openAPIDocument is the canonical description of every route in routes().
//
//go:embed openapi.yaml
var openAPIDocument []byte

// docsViewerPage renders openAPIDocument with a pinned, integrity-checked Swagger UI release.
const docsViewerPage = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Data Fetch Service API</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.17.14/swagger-ui.css" integrity="sha384-wxLW6kwyHktdDGr6Pv1zgm/VGJh99lfUbzSn6HNHBENZlCN7W602k9VkGdxuFvPn" crossorigin="anonymous">
</head>
<body>
<div id="swagger-ui"></div>
<script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.17.14/swagger-ui-bundle.js" integrity="sha384-wmyclcVGX/WhUkdkATwhaK1X1JtiNrr2EoYJ+diV3vj4v6OC5yCeSu+yW13SYJep" crossorigin="anonymous"></script>
<script>
SwaggerUIBundle({url: "/docs/openapi.yaml", dom_id: "#swagger-ui", deepLinking: true});
</script>
</body>
</html>
`

// serveOpenAPI returns the embedded OpenAPI document unchanged.
func serveOpenAPI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/yaml")
	w.Header().Set("Cache-Control", "public, max-age=300")
	if _, err := w.Write(openAPIDocument); err != nil {
		return
	}
}

// serveDocsViewer returns the Swagger UI page; its scripts may load only from the pinned CDN.
func serveDocsViewer(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src https://cdn.jsdelivr.net 'unsafe-inline'; style-src https://cdn.jsdelivr.net 'unsafe-inline'; img-src 'self' data: https://cdn.jsdelivr.net; connect-src 'self'")
	w.Header().Set("Cache-Control", "public, max-age=300")
	if _, err := w.Write([]byte(docsViewerPage)); err != nil {
		return
	}
}
