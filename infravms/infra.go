package infravms

import (
	"context"
	"log"
)

type Infra interface {
	ScaleUp(instanceGroupName string, count int64) error
	ScaleDown(instanceGroupName string, count int64) error
}

func NewInfraClient(ctx context.Context, projectID, zone string,
	logger log.Logger) (Infra, error) {

	return newGCPInfraClient(ctx, logger, GCPParams{
		ProjectID: projectID,
		Zone:      zone,
	})
}
