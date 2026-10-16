package main

import (
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/layer5io/meshkit/logger"
	"github.com/layer5io/meshkit/utils"
	"github.com/layer5io/meshkit/utils/events"

	// "github.com/layer5io/meshkit/tracing"
	"github.com/layer5io/meshery-adapter-library/adapter"
	"github.com/layer5io/meshery-adapter-library/api/grpc"
	"github.com/layer5io/meshery-kuma/build"
	"github.com/layer5io/meshery-kuma/internal/config"
	"github.com/layer5io/meshery-kuma/kuma"
	"github.com/layer5io/meshery-kuma/kuma/oam"
	configprovider "github.com/layer5io/meshkit/config/provider"
)

var (
	serviceName = "kuma-adapter"
	version     = "edge"
	gitsha      = "none"
	instanceID  = uuid.NewString()
)

// main is the entrypoint of the adapter
func main() {
	// Initialize Logger instance
	log, err := logger.New(serviceName, logger.Options{
		Format: logger.SyslogLogFormat,
	})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Initialize application specific configs and dependencies
	// App and request config
	cfg, err := config.New(configprovider.ViperKey)
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}

	service := &grpc.Service{}
	err = cfg.GetObject(adapter.ServerKey, service)
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}

	kubeconfigHandler, err := config.NewKubeconfigBuilder(configprovider.ViperKey)
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
	e := events.NewEventStreamer()
	// Initialize Handler intance
	handler := kuma.New(cfg, log, kubeconfigHandler, e)
	handler = adapter.AddLogger(log, handler)

	service.Handler = handler
	service.EventStreamer = e
	service.StartedAt = time.Now()
	service.Version = version
	service.GitSHA = gitsha

	go registerCapabilities(service.Port, log)        //Registering static capabilities
	go registerDynamicCapabilities(service.Port, log) //Registering latest capabilities periodically

	// Server Initialization
	log.Info("Adapter listening on port: ", service.Port)
	err = grpc.Start(service, nil)
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
}

func init() {
	err := os.MkdirAll(path.Join(utils.GetHome(), ".meshery", "bin"), 0750)
	if err != nil {
		fmt.Println(err)
		os.Exit(0)
	}

	err = os.Setenv("KUBECONFIG", fmt.Sprintf("%s/.meshery/kubeconfig.yaml", utils.GetHome()))
	if err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
}

func mesheryServerAddress() string {
	meshReg := os.Getenv("MESHERY_SERVER")

	if meshReg != "" {
		if strings.HasPrefix(meshReg, "http") {
			return meshReg
		}

		return "http://" + meshReg
	}

	return "http://localhost:9081"
}

func serviceAddress() string {
	svcAddr := os.Getenv("SERVICE_ADDR")

	if svcAddr != "" {
		return svcAddr
	}

	return "localhost"
}

func registerCapabilities(port string, log logger.Handler) {
	// Register meshmodel components
	if err := oam.RegisterMeshModelComponents(instanceID, mesheryServerAddress(), serviceAddress(), port, build.LatestVersion); err != nil {
		log.Info(err.Error())
	}
}

func registerDynamicCapabilities(port string, log logger.Handler) {
	registerWorkloads(port, log)
	//Start the ticker
	const reRegisterAfter = 24
	ticker := time.NewTicker(reRegisterAfter * time.Hour)
	for {
		<-ticker.C
		registerWorkloads(port, log)
	}
}

func registerWorkloads(port string, log logger.Handler) {
	//First we create and store any new components if available
	version := build.LatestVersion
	url := build.DefaultGenerationURL
	gm := build.DefaultGenerationMethod
	// Prechecking to skip comp gen
	if os.Getenv("FORCE_DYNAMIC_REG") != "true" && oam.AvailableVersions[version] {
		log.Info("Components available statically for version ", version, ". Skipping dynamic component registeration")
		return
	}
	//If a URL is passed from env variable, it will be used for component generation with default method being "using manifests"
	// In case a helm chart URL is passed, COMP_GEN_METHOD env variable should be set to Helm otherwise the component generation fails
	if os.Getenv("COMP_GEN_URL") != "" && (os.Getenv("COMP_GEN_METHOD") == "Helm" || os.Getenv("COMP_GEN_METHOD") == "Manifest") {
		url = os.Getenv("COMP_GEN_URL")
		gm = os.Getenv("COMP_GEN_METHOD")
		log.Info("Registering workload components from url ", url, " using ", gm, " method...")
	}
	log.Info("Registering latest workload components for version ", version)
	// Register workloads
	if err := adapter.CreateComponents(adapter.StaticCompConfig{
		URL:             url,
		Method:          gm,
		MeshModelPath:   build.MeshModelPath,
		MeshModelConfig: build.MeshModelConfig,
		DirName:         version,
		Config:          build.NewConfig(version),
	}); err != nil {
		log.Info("Failed to generate components for version "+version, "ERR: ", err.Error())
		return
	}
	//The below log is checked in the workflows. If you change this log, reflect that change in the workflow where components are generated
	log.Info("Component creation completed for version ", version)

	//Now we will register in case
	log.Info("Registering workloads with Meshery Server for version ", version)
	if err := oam.RegisterMeshModelComponents(instanceID, mesheryServerAddress(), serviceAddress(), port, version); err != nil {
		log.Info(err.Error())
		return
	}
	log.Info("Registered latest service mesh components with Meshery Server at ", mesheryServerAddress())
}
