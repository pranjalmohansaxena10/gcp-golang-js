package secrets

import (
	"context"
	"log"
	"strings"
)

type SecretsPayloadReq struct {
	OrgID   string   `json:"orgId"`
	User    string   `json:"username"`
	Secrets []Secret `json:"secrets"`
}
type Secret struct {
	Key   string `json:"secretKey"`
	Value string `json:"secretValue"`
}

type SecretManager interface {
	GetSecret(payload map[string]string) (map[string]interface{}, error)
	SetSecret(payload map[string]string) error
	SetSecrets(payload *SecretsPayloadReq) error
	DeleteSecret(payload map[string]string) error
	DeleteSecrets(payload *SecretsPayloadReq) error
}

// NewSecretsClient returns new storage client
func NewSecretsClient(ctx context.Context, cloudProvider, projectID string,
	logger log.Logger) (SecretManager, error) {

	switch strings.ToLower(cloudProvider) {
	case "gcs":
		return newGSMClient(ctx, GSMClientParams{
			ProjectID: projectID,
			Logger:    logger,
			Ctx:       ctx,
		})
	default:
		return newGSMClient(ctx, GSMClientParams{
			ProjectID: projectID,
			Logger:    logger,
			Ctx:       ctx,
		})
	}
}
