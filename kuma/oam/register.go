package oam

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	backoff "github.com/cenkalti/backoff/v4"
	"github.com/layer5io/meshery-adapter-library/adapter"
	"github.com/layer5io/meshkit/models/meshmodel/core/types"
)

var (
	basePath, _         = os.Getwd()
	MeshmodelComponents = filepath.Join(basePath, "templates", "meshmodel", "components")
)

// AvailableVersions denote the component versions available statically
var AvailableVersions = map[string]bool{}
var availableVersionGlobalMutex sync.Mutex

type meshmodelDefinitionPathSet struct {
	meshmodelDefinitionPath string
}

type legacyComponentDefinition struct {
	Kind        string `json:"kind"`
	APIVersion  string `json:"apiVersion"`
	DisplayName string `json:"displayName"`
	Format      string `json:"format"`
	Schema      string `json:"schema"`
	Model       struct {
		Name        string `json:"name"`
		Version     string `json:"version"`
		DisplayName string `json:"displayName"`
		Category    struct {
			Name string `json:"name"`
		} `json:"category"`
	} `json:"model"`
}

type meshModelRegistrantData struct {
	Connection map[string]interface{} `json:"connection"`
	EntityType string                 `json:"entityType"`
	Entity     []byte                 `json:"entity"`
}

func RegisterMeshModelComponents(uuid, runtime, host, port, latestVersion string) error {
	pathSets, err := loadMeshmodelComponents(MeshmodelComponents)
	if err != nil {
		return err
	}
	portint, _ := strconv.Atoi(port)
	// Register the latest KumaMesh definition first, then the remaining
	// KumaMesh definitions, before the full catalog is ready for patterns.
	latestKumaMesh := filepath.Join(MeshmodelComponents, latestVersion, "kumamesh_meshmodel.json")
	sort.SliceStable(pathSets, func(i, j int) bool {
		pathI := pathSets[i].meshmodelDefinitionPath
		pathJ := pathSets[j].meshmodelDefinitionPath
		if (pathI == latestKumaMesh) != (pathJ == latestKumaMesh) {
			return pathI == latestKumaMesh
		}
		return filepath.Base(pathI) == "kumamesh_meshmodel.json" &&
			filepath.Base(pathJ) != "kumamesh_meshmodel.json"
	})

	registryURL := fmt.Sprintf("%s/api/registry/components", strings.TrimRight(runtime, "/"))
	meshmodelRDP := []adapter.MeshModelRegistrantDefinitionPath{}
	for _, pathSet := range pathSets {
		if pathSet.meshmodelDefinitionPath == latestKumaMesh {
			if err := registerComponentDefinition(uuid, registryURL, host, portint, pathSet.meshmodelDefinitionPath); err != nil {
				return err
			}
			continue
		}
		meshmodelRDP = append(meshmodelRDP, adapter.MeshModelRegistrantDefinitionPath{
			EntityDefintionPath: pathSet.meshmodelDefinitionPath,
			Host:                host,
			Port:                portint,
			Type:                types.ComponentDefinition,
		})
	}

	return adapter.
		NewMeshModelRegistrant(meshmodelRDP, fmt.Sprintf("%s/api/meshmodel/components/register", runtime)).
		Register(uuid)
}

func registerComponentDefinition(contextID, registryURL, host string, port int, definitionPath string) error {
	definitionFile, err := os.ReadFile(definitionPath)
	if err != nil {
		return fmt.Errorf("read component definition %q: %w", definitionPath, err)
	}

	var legacy legacyComponentDefinition
	if err := json.Unmarshal(definitionFile, &legacy); err != nil {
		return fmt.Errorf("parse component definition %q: %w", definitionPath, err)
	}
	if legacy.DisplayName == "" {
		legacy.DisplayName = legacy.Kind
	}
	if legacy.Model.DisplayName == "" {
		legacy.Model.DisplayName = legacy.Model.Name
	}
	if legacy.Kind == "" || legacy.APIVersion == "" || legacy.Model.Name == "" || legacy.Model.Version == "" || legacy.Schema == "" {
		return fmt.Errorf("component definition %q is missing kind, apiVersion, model name, model version, or schema", definitionPath)
	}
	if legacy.Format == "" {
		legacy.Format = "JSON"
	}

	component := map[string]interface{}{
		"schemaVersion": "components.meshery.io/v1beta2",
		"version":       "v1.0.0",
		"displayName":   legacy.DisplayName,
		"format":        legacy.Format,
		"model": map[string]interface{}{
			"schemaVersion": "models.meshery.io/v1beta1",
			"version":       "v1.0.0",
			"name":          legacy.Model.Name,
			"displayName":   legacy.Model.DisplayName,
			"status":        "enabled",
			"registrant":    map[string]string{"kind": "meshery-kuma"},
			"category":      map[string]string{"name": legacy.Model.Category.Name},
			"model":         map[string]string{"version": legacy.Model.Version},
		},
		"component": map[string]string{
			"kind":    legacy.Kind,
			"version": legacy.APIVersion,
			"schema":  legacy.Schema,
		},
		"status":   "enabled",
		"metadata": map[string]bool{"published": true, "isAnnotation": false},
	}
	entity, err := json.Marshal(component)
	if err != nil {
		return fmt.Errorf("encode component definition %q: %w", definitionPath, err)
	}

	requestBody, err := json.Marshal(meshModelRegistrantData{
		Connection: map[string]interface{}{
			"id":            contextID,
			"name":          "Meshery Kuma Adapter",
			"type":          "platform",
			"subType":       "adapter",
			"kind":          "meshery-kuma",
			"status":        "registered",
			"schemaVersion": "connections.meshery.io/v1beta3",
			"metadata":      map[string]interface{}{"hostname": host, "port": port, "contextID": contextID},
		},
		EntityType: "component",
		Entity:     entity,
	})
	if err != nil {
		return fmt.Errorf("encode registry request for %q: %w", definitionPath, err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	backoffPolicy := backoff.NewExponentialBackOff()
	backoffPolicy.MaxElapsedTime = 10 * time.Minute
	return backoff.Retry(func() error {
		request, err := http.NewRequest(http.MethodPost, registryURL, bytes.NewReader(requestBody))
		if err != nil {
			return backoff.Permanent(fmt.Errorf("create registry request for %q: %w", definitionPath, err))
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := client.Do(request)
		if err != nil {
			return err
		}
		defer func() { _ = response.Body.Close() }()
		if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
			body, _ := io.ReadAll(response.Body)
			err := fmt.Errorf("registry rejected component %q with %s: %s", legacy.Kind, response.Status, strings.TrimSpace(string(body)))
			if response.StatusCode >= http.StatusBadRequest && response.StatusCode < http.StatusInternalServerError && response.StatusCode != http.StatusTooManyRequests {
				return backoff.Permanent(err)
			}
			return err
		}
		return nil
	}, backoffPolicy)
}

func loadMeshmodelComponents(basepath string) ([]meshmodelDefinitionPathSet, error) {
	res := []meshmodelDefinitionPathSet{}
	if err := filepath.Walk(basepath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		res = append(res, meshmodelDefinitionPathSet{
			meshmodelDefinitionPath: path,
		})
		availableVersionGlobalMutex.Lock()
		AvailableVersions[filepath.Base(filepath.Dir(path))] = true // Getting available versions already existing on file system
		availableVersionGlobalMutex.Unlock()
		return nil
	}); err != nil {
		return nil, err
	}

	return res, nil
}
