package delivery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

// SigV4Signer signs the immutable processor submission for API Gateway IAM authorization.
type SigV4Signer struct {
	Credentials aws.CredentialsProvider
	Region      string
	Now         func() time.Time
}

// Sign adds execute-api SigV4 headers using the exact request body hash.
func (s *SigV4Signer) Sign(ctx context.Context, request *http.Request, body []byte) error {
	if s == nil || s.Credentials == nil || s.Region == "" || s.Now == nil || request == nil || request.URL == nil || request.URL.Scheme != "https" {
		return fmt.Errorf("processor signing configuration invalid")
	}
	credentials, err := s.Credentials.Retrieve(ctx)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(body)
	return v4.NewSigner().SignHTTP(ctx, credentials, request, hex.EncodeToString(digest[:]), "execute-api", s.Region, s.Now().UTC())
}
