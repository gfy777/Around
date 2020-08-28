package main

import (
	vision "cloud.google.com/go/vision/apiv1"
	"context"
	"fmt"
)

// detectFaces gets faces from the Vision API for an image at the given file path.
func detectFacesURI(file string) (bool, error) {
	ctx := context.Background()

	client, err := vision.NewImageAnnotatorClient(ctx)
	if err != nil {
		return false, err
	}

	image := vision.NewImageFromURI(file)
	annotations, err := client.DetectFaces(ctx, image, nil, 10)
	if err != nil {
		return false, err
	}
	if len(annotations) == 0 {
		fmt.Println("No faces found.")
	}
	return true, nil
}
