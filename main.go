package main

import (
	"context"
	"fmt"

	"github.com/pokt-foundation/utils-go/environment"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/pokt-foundation/wss-rewards/router"
)

const (
	// Optional env variables
	port            = "PORT"
	defaultPort     = "8080"
	imageTag        = "IMAGE_TAG"
	defaultImageTag = "development"
)

type options struct {
	// Optional env variables
	port     string
	imageTag string
}

func gatherOptions() options {
	return options{
		// Optional env variables
		port:     environment.GetString(port, defaultPort),
		imageTag: environment.GetString(imageTag, defaultImageTag),
	}
}

func main() {
	options := gatherOptions()

	logger := logger.New()

	err := router.Start(context.Background(), router.Config{
		Port:     options.port,
		ImageTag: options.imageTag,
		Logger:   logger,
	})
	if err != nil {
		logger.Error(fmt.Sprintf("create API router failed with error: %s", err.Error()))
		panic(err)
	}
}
