package acquisition

import (
	"github.com/maverickuser/data-fetch-service/internal/config"
	"time"
)

// ConfiguredLimits copies the admitted configuration into source and parser budgets.
func ConfiguredLimits(d config.Defaults) Limits {
	return Limits{MaxDownloadBytes: d.MaxDownloadBytes, MaxExtractedBytes: d.MaxExtractedBytes, MaxTokenBytes: d.MaxValidationTokenBytes, MaxZipMetadataBytes: d.MaxZipMetadataBytes, MaxZipEntries: d.MaxZipEntries, MaxCompressionRatio: d.MaxCompressionRatio, MaxJSONDepth: d.MaxJSONDepth, MaxAttempts: d.SourceMaxAttempts, RequestTimeout: time.Duration(d.RequestTimeoutSeconds) * time.Second}
}
