package infravms

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/api/compute/v1"
)

type GCPParams struct {
	ProjectID string
	Zone      string
}
type gcpInfra struct {
	logger                  log.Logger
	ctx                     context.Context
	computeService          *compute.Service
	instanceGroupManagerSvc *compute.InstanceGroupManagersService
	mutex                   sync.Mutex
	scalingMap              *sync.Map
	projectID               string
	zone                    string
}

func newGCPInfraClient(ctx context.Context, logger log.Logger, params GCPParams) (Infra, error) {
	gcpService, err := compute.NewService(ctx)
	if err != nil {
		log.Fatalf("failed to create client for gcp service since: %+v", err)
		return nil, err
	}

	return &gcpInfra{
		logger:                  logger,
		ctx:                     ctx,
		computeService:          gcpService,
		instanceGroupManagerSvc: compute.NewInstanceGroupManagersService(gcpService),
		mutex:                   sync.Mutex{},
		scalingMap:              &sync.Map{},
		projectID:               params.ProjectID,
		zone:                    params.Zone,
	}, nil
}

func (g *gcpInfra) ScaleUp(instanceGroupName string, count int64) error {
	// Check if given instance group is already being scaled
	_, loaded := g.scalingMap.LoadOrStore(instanceGroupName, true)
	if loaded {
		return fmt.Errorf("instance Group %q is already being scaled hence, skipping this request", instanceGroupName)
	}
	defer g.scalingMap.Delete(instanceGroupName)

	g.mutex.Lock()
	defer g.mutex.Unlock()

	g.logger.Printf("Scaling up instance group...")
	currentSize, err := g.getInstanceGroupSize(instanceGroupName)
	if err != nil {
		g.logger.Printf("Error getting managed instance group from GCP: %v", err)
		return err
	}

	g.logger.Printf("currentSize: %+v", currentSize)
	updatedReplicaCount := currentSize + count
	g.logger.Printf("updatedReplicaCount: %+v", updatedReplicaCount)

	// Get the autoscaler for the MIG.
	autoscaler, err := g.computeService.Autoscalers.Get(g.projectID, g.zone, instanceGroupName).Do()
	if err != nil {
		g.logger.Printf("Failed to get autoscaler: %+v", err)
		return err
	}
	maxInstances := autoscaler.AutoscalingPolicy.MaxNumReplicas

	g.logger.Printf("maxInstances: %+v", maxInstances)

	// Taking min of currentSize + count or maxInstances
	if updatedReplicaCount > maxInstances {
		updatedReplicaCount = maxInstances
		g.logger.Printf("Since requested count exceeds the max replicas hence using count as %+v", updatedReplicaCount)
	}

	operation, err := g.instanceGroupManagerSvc.Resize(g.projectID, g.zone, instanceGroupName, updatedReplicaCount).Context(g.ctx).Do()
	if err != nil {
		g.logger.Printf("Failed to scale up given instance group: %v", err)
		return err
	}

	// Wait for the resize operation to complete.
	g.logger.Printf("Waiting for scaling up operation %s to be completed...", operation.Name)
	if err := g.waitForOperation(g.ctx, operation.Name); err != nil {
		g.logger.Printf("Scaling up operation failed with err: %v", err)
		return err
	}

	g.logger.Printf("Instance group %+v is scaled successfully.", instanceGroupName)

	return nil
}

func (g *gcpInfra) ScaleDown(instanceGroupName string, count int64) error {
	// Check if given instance group is already being scaled
	_, loaded := g.scalingMap.LoadOrStore(instanceGroupName, true)
	if loaded {
		return fmt.Errorf("instance Group %q is already being scaled hence, skipping this request", instanceGroupName)
	}
	defer g.scalingMap.Delete(instanceGroupName)

	g.mutex.Lock()
	defer g.mutex.Unlock()

	g.logger.Printf("Scaling down instance group...")
	currentSize, err := g.getInstanceGroupSize(instanceGroupName)
	if err != nil {
		g.logger.Printf("Error getting managed instance group from GCP: %v", err)
		return err
	}

	g.logger.Printf("currentSize: %+v", currentSize)
	updatedReplicaCount := currentSize - count
	g.logger.Printf("updatedReplicaCount: %+v", updatedReplicaCount)

	// Get the autoscaler for the MIG.
	autoscaler, err := g.computeService.Autoscalers.Get(g.projectID, g.zone, instanceGroupName).Do()
	if err != nil {
		g.logger.Printf("Failed to get autoscaler: %+v", err)
		return err
	}
	minInstances := autoscaler.AutoscalingPolicy.MinNumReplicas

	g.logger.Printf("minInstances: %+v", minInstances)

	// Taking max of currentSize - count or minInstances
	if updatedReplicaCount < minInstances {
		updatedReplicaCount = minInstances
		g.logger.Printf("Since requested count is lower than the min replicas hence using count as %+v", updatedReplicaCount)
	}

	operation, err := g.instanceGroupManagerSvc.Resize(g.projectID, g.zone, instanceGroupName, updatedReplicaCount).Context(g.ctx).Do()
	if err != nil {
		g.logger.Printf("Failed to scale down given instance group: %v", err)
		return err
	}

	// Wait for the resize operation to complete.
	g.logger.Printf("Waiting for scaling down operation %s to be completed...", operation.Name)
	if err := g.waitForOperation(g.ctx, operation.Name); err != nil {
		g.logger.Printf("Scaling down operation failed with err: %v", err)
		return err
	}

	g.logger.Printf("Instance group %+v is scaled successfully.", instanceGroupName)
	return nil
}

func (g *gcpInfra) waitForOperation(ctx context.Context, operationName string) error {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context timed out while waiting for operation %s to be completed", operationName)
		case <-ticker.C:

			operation, err := g.computeService.ZoneOperations.Get(g.projectID, g.zone, operationName).Context(ctx).Do()
			if err != nil {
				return fmt.Errorf("failed to get operation status of %s: %v", operationName, err)
			}
			instanceGroupManager, err := g.computeService.InstanceGroupManagers.
				Get(g.projectID, g.zone, "dev-poc-instance-group-1").Context(g.ctx).Do()
			if err != nil {
				return fmt.Errorf("failed to get operation status of %s: %v", operationName, err)
			}
			if instanceGroupManager.Status.IsStable {
				g.logger.Printf("Operation %s completed successfully", operation.Name)
			}

			switch operation.Status {
			case "PENDING", "RUNNING":
				g.logger.Printf("status: %s of operation %s ...", operation.Name, operation.Status)
			case "DONE":

				if operation.Error != nil {
					return fmt.Errorf("operation %s got failed: %v", operation.Name, operation.Error.Errors)
				}
				return nil
			default:
				return fmt.Errorf("operation %s ran into invalid status: %s", operation.Name, operation.Status)
			}
		}
	}
}

func (g *gcpInfra) getInstanceGroupSize(instanceGroupName string) (int64, error) {
	gcpInstanceGroup, err := g.computeService.InstanceGroups.
		Get(g.projectID, g.zone, instanceGroupName).Do()
	if err != nil {
		return 0, err
	}
	return gcpInstanceGroup.Size, nil
}
