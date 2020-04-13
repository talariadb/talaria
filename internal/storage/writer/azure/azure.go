package azure

import (
	"os"
	"path"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/kelindar/talaria/internal/monitor/errors"
)

// Writer represents a writer for Microsoft Azure.
type Writer struct {
	prefix    string
	container *storage.Container
}

// New creates a new writer.
func New(container, prefix string) (*Writer, error) {

	// From the Azure portal, get your storage account name and key and set environment variables.
	accountName, accountKey := os.Getenv("AZURE_STORAGE_ACCOUNT"), os.Getenv("AZURE_STORAGE_ACCESS_KEY")
	var serviceBaseURL, apiVersion string
	if serviceBaseURL = os.Getenv("AZURE_BASE_URL"); serviceBaseURL == "" {
		serviceBaseURL = storage.DefaultBaseURL
	}

	if apiVersion = os.Getenv("AZURE_API_VERSION"); apiVersion == "" {
		apiVersion = storage.DefaultAPIVersion
	}

	if len(accountName) == 0 || len(accountKey) == 0 {
		return nil, errors.New("azure: either the AZURE_STORAGE_ACCOUNT or AZURE_STORAGE_ACCESS_KEY environment variable is not set")
	}

	// Create a new storage client
	client, err := storage.NewClient(accountName, accountKey, serviceBaseURL, apiVersion, true)
	if err != nil {
		return nil, errors.Internal("azure: unable to create a client", err)
	}

	svc := client.GetBlobService()
	ref := svc.GetContainerReference(container)
	return &Writer{
		prefix:    prefix,
		container: ref,
	}, nil
}

// Write writes the data to the sink.
func (w *Writer) Write(key key.Key, val []byte) error {
	if w.container == nil {
		return errors.New("azure: unable to obtain a container reference")
	}

	ref := w.container.GetBlobReference(path.Join(w.prefix, string(key)))
	if err := ref.PutAppendBlob(nil); err != nil {
		return errors.Internal("azure: unable to write", err)
	}

	if err := ref.AppendBlock(val, nil); err != nil {
		return errors.Internal("azure: unable to write", err)
	}
	return nil
}
