package acquisition

import (
	"fmt"
	"mime"
	"net/url"
	"path"
	"strings"
	"unicode"
)

// filename selects explicit, original URL, response header, then configured job-ID names.
func filename(explicit, sourceURL, disposition, jobID, format string) (string, error) {
	if explicit != "" {
		if !safeFilename(explicit, format) {
			return "", fmt.Errorf("unsafe explicit filename")
		}
		return explicit, nil
	}
	u, err := url.Parse(sourceURL)
	if err != nil {
		return "", err
	}
	if name, err := url.PathUnescape(path.Base(u.EscapedPath())); err == nil && safeFilename(name, format) {
		return name, nil
	}
	if _, params, err := mime.ParseMediaType(disposition); err == nil {
		if name := params["filename"]; safeFilename(name, format) {
			return name, nil
		}
	}
	name := jobID + "." + format
	if !safeFilename(name, format) {
		return "", fmt.Errorf("unsafe fallback filename")
	}
	return name, nil
}

// safeFilename accepts a single decoded filename with the expected content extension.
func safeFilename(name, format string) bool {
	return name != "" && name != "." && name != ".." && !strings.ContainsAny(name, "/\\:") && strings.IndexFunc(name, unicode.IsControl) < 0 && strings.EqualFold(path.Ext(name), "."+format)
}

// safeMember rejects archive traversal and platform-dependent path separators.
func safeMember(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.HasPrefix(name, "/") && !strings.HasPrefix(name, "../") && path.Clean(name) == name && !strings.ContainsAny(name, "\\:") && strings.IndexFunc(name, unicode.IsControl) < 0
}
