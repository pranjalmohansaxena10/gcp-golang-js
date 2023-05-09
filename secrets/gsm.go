package secrets

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type gsmClient struct {
	projectID string
	logger    log.Logger
	client    *secretmanager.Client
	ctx       context.Context
}

type GSMClientParams struct {
	ProjectID string
	Logger    log.Logger
	Ctx       context.Context
}

func newGSMClient(ctx context.Context, params GSMClientParams) (SecretManager, error) {
	if params.ProjectID == "" {
		return nil, errors.New("missing GCP project id")
	}
	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		log.Fatalf("failed to setup client: %v", err)
	}

	return gsmClient{
		projectID: params.ProjectID,
		logger:    params.Logger,
		client:    client,
		ctx:       params.Ctx,
	}, nil
}

func (g gsmClient) GetSecret(payload map[string]string) (map[string]interface{}, error) {
	secrets := make(map[string]interface{})
	secretName := g.getSecretName(payload["username"])
	response, err := g.client.AccessSecretVersion(g.ctx, &secretmanagerpb.AccessSecretVersionRequest{
		Name: fmt.Sprintf("projects/%s/secrets/%s/versions/latest", g.projectID, secretName),
	})
	if err != nil {
		g.logger.Printf("Error accessing secret for user from GSM %s, error - %v", payload["username"], err.Error())
		return secrets, err
	}

	if response.Payload.Data != nil {
		err = json.Unmarshal([]byte(response.Payload.Data), &secrets)
		if err != nil {
			g.logger.Printf("Error unmarshalling secret value for user %s, error - %v", payload["username"], err.Error())
			return secrets, err
		}
	}

	return secrets, nil
}

// this func will generate secret name for google secret manager
func (g *gsmClient) getSecretName(username string) string {
	mask := md5.Sum([]byte(username))
	return "hyperexecute-secrets-" + hex.EncodeToString(mask[:])
}

func (g gsmClient) SetSecret(payload map[string]string) error {
	req := &SecretsPayloadReq{
		OrgID: payload["orgId"],
		User:  payload["username"],
		Secrets: []Secret{
			{
				Key:   payload["secretKey"],
				Value: payload["secretValue"],
			},
		},
	}
	return g.SetSecrets(req)
}

func (g gsmClient) SetSecrets(payload *SecretsPayloadReq) error {
	if payload == nil {
		return errors.New("missing SecretsPayloadReq payload")
	}
	// Check if secret exists, if exists put a secret else create a new secret
	secretName := g.getSecretName(payload.User)
	secrets, err := g.GetSecret(map[string]string{"username": payload.User})
	if err != nil {
		if status, ok := status.FromError(err); ok && status.Code() == codes.NotFound {
			// If the secret does not exist, create it.
			_, err := g.client.CreateSecret(g.ctx, &secretmanagerpb.CreateSecretRequest{
				Parent:   fmt.Sprintf("projects/%s", g.projectID),
				SecretId: secretName,
				Secret: &secretmanagerpb.Secret{
					Replication: &secretmanagerpb.Replication{
						Replication: &secretmanagerpb.Replication_Automatic_{
							Automatic: &secretmanagerpb.Replication_Automatic{},
						},
					},
				},
			})
			if err != nil {
				g.logger.Printf("Error creating secret for user in GSM %s, error - %v", payload.User, err.Error())
				return err
			}

			for idx := 0; idx < len(payload.Secrets); idx += 1 {
				secrets[payload.Secrets[idx].Key] = payload.Secrets[idx].Value
			}
			secretJSON, err := json.Marshal(secrets)
			if err != nil {
				g.logger.Printf("Error marshaling json for user - %s", payload.User)
				return err
			}
			_, err = g.client.AddSecretVersion(g.ctx, &secretmanagerpb.AddSecretVersionRequest{
				Parent: fmt.Sprintf("projects/%s/secrets/%s", g.projectID, secretName),
				Payload: &secretmanagerpb.SecretPayload{
					Data: secretJSON,
				},
			})
			if err != nil {
				g.logger.Printf("Error adding payload to GSM secret for user - %s since - %+v", payload.User, err)
				return err
			}
			log.Printf("Created secret for %q user\n", payload.User)
		}
	} else {
		for idx := 0; idx < len(payload.Secrets); idx += 1 {
			secrets[payload.Secrets[idx].Key] = payload.Secrets[idx].Value
		}
		secretJSON, err := json.Marshal(secrets)
		if err != nil {
			g.logger.Printf("Error marshaling json for user - %s", payload.User)
			return err
		}
		_, err = g.client.AddSecretVersion(g.ctx, &secretmanagerpb.AddSecretVersionRequest{
			Parent: fmt.Sprintf("projects/%s/secrets/%s", g.projectID, secretName),
			Payload: &secretmanagerpb.SecretPayload{
				Data: secretJSON,
			},
		})
		if err != nil {
			g.logger.Printf("Error adding payload to GSM secret for user - %s since - %+v", payload.User, err)
			return err
		}
		log.Printf("Updated secret for %q user\n", payload.User)
	}
	return nil
}

func (g gsmClient) DeleteSecret(payload map[string]string) error {
	req := &SecretsPayloadReq{
		OrgID: payload["orgId"],
		User:  payload["username"],
		Secrets: []Secret{
			{
				Key:   payload["secretKey"],
				Value: payload["secretValue"],
			},
		},
	}
	return g.DeleteSecrets(req)
}

func (g gsmClient) DeleteSecrets(payload *SecretsPayloadReq) error {
	if payload == nil {
		return errors.New("missing SecretsPayloadReq payload")
	}
	// Check if secret exists
	secretName := g.getSecretName(payload.User)
	existingData, err := g.GetSecret(map[string]string{"username": payload.User})
	if err != nil {
		return err
	}

	for idx := 0; idx < len(payload.Secrets); idx += 1 {
		delete(existingData, payload.Secrets[idx].Key)
	}

	if len(existingData) > 0 {
		secretJSON, err := json.Marshal(existingData)
		if err != nil {
			g.logger.Printf("Error marshaling json for user - %s", payload.User)
			return err
		}
		_, err = g.client.AddSecretVersion(g.ctx, &secretmanagerpb.AddSecretVersionRequest{
			Parent: fmt.Sprintf("projects/%s/secrets/%s", g.projectID, secretName),
			Payload: &secretmanagerpb.SecretPayload{
				Data: secretJSON,
			},
		})
		if err != nil {
			g.logger.Printf("Error adding payload after deleting secret for user - %s since - %+v", payload.User, err)
			return err
		}
		g.logger.Printf("Updated secret after deleting given secrets for %q user\n", payload.User)
	} else {
		err = g.client.DeleteSecret(g.ctx, &secretmanagerpb.DeleteSecretRequest{
			Name: fmt.Sprintf("projects/%s/secrets/%s", g.projectID, secretName),
		})
		if err != nil {
			g.logger.Printf("Error deleting secret for user - %s since - %+v", payload.User, err)
			return err
		}
		g.logger.Printf("Deleted secret for %q user\n", payload.User)
	}

	return nil
}
