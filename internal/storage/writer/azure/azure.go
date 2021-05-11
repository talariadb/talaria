package azure

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"path"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/go-autorest/autorest/azure/auth"
	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/errors"
	"github.com/mroth/weightedrand"
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

const (
	ctxTag                = "azure"
	tokenRefreshBuffer    = 2 * time.Minute
	defaultBlobServiceURL = "https://%s.blob.core.windows.net"
	defaultResourceID     = "https://storage.azure.com/"
)

// MultiAccountWriter represents a writer for Microsoft Azure with multiple storage accounts.
type MultiAccountWriter struct {
	monitor        monitor.Monitor
	blobServiceURL string
	prefix         string
	containerURLs  []azblob.ContainerURL
	options        azblob.UploadToBlockBlobOptions
	chooser        *weightedrand.Chooser
}

// NewMultiAccountWriter creates a new MultiAccountWriter.
func NewMultiAccountWriter(monitor monitor.Monitor, blobServiceURL, container, prefix string, storageAccount []string, weights []uint, parallelism uint16, blockSize int64) (*MultiAccountWriter, error) {
	if _, present := os.LookupEnv("AZURE_AD_RESOURCE"); !present {
		if err := os.Setenv("AZURE_AD_RESOURCE", defaultResourceID); err != nil {
			return nil, errors.New("azure: unable to set default AZURE_AD_RESOURCE environment variable")
		}
	}
	if blobServiceURL == "" {
		blobServiceURL = defaultBlobServiceURL
	}

	credential, err := GetAzureStorageCredentials(monitor)
	if err != nil {
		return nil, errors.Internal("azure: unable to get azure storage credential", err)
	}

	containerURLs := make([]azblob.ContainerURL, len(storageAccount))
	for i, sa := range storageAccount {
		azureStoragePipeline := azblob.NewPipeline(credential, azblob.PipelineOptions{
			Retry: azblob.RetryOptions{
				MaxTries: 3,
			},
		})
		u, _ := url.Parse(fmt.Sprintf(blobServiceURL, sa))
		containerURLs[i] = azblob.NewServiceURL(*u, azureStoragePipeline).NewContainerURL(container)
	}

	var chooser *weightedrand.Chooser
	if weights != nil {

		if len(storageAccount) != len(weights) {
			return nil, fmt.Errorf("Invalid configuration number of storage account %v !=  number of weights %v", len(storageAccount), len(weights))
		}

		choices := make([]weightedrand.Choice, len(storageAccount))
		for i, w := range weights {
			choices[i] = weightedrand.Choice{
				Item:   &containerURLs[i],
				Weight: w,
			}
		}
		chooser, err = weightedrand.NewChooser(choices...)
		if err != nil {
			return nil, err
		}
	}

	return &MultiAccountWriter{
		monitor:       monitor,
		prefix:        prefix,
		containerURLs: containerURLs,
		options: azblob.UploadToBlockBlobOptions{
			Parallelism: parallelism,
			BlockSize:   blockSize,
		},
		chooser: chooser,
	}, nil
}

func GetAzureStorageCredentials(monitor monitor.Monitor) (azblob.Credential, error) {
	settings, err := auth.GetSettingsFromEnvironment()
	if err != nil {
		return nil, err
	}

	cc, err := settings.GetClientCredentials()
	if err != nil {
		return nil, err
	}

	spt, err := cc.ServicePrincipalToken()
	if err != nil {
		return nil, err
	}

	// Refresh the token once
	if err := spt.Refresh(); err != nil {
		return nil, err
	}

	// Token refresher function
	var tokenRefresher azblob.TokenRefresher
	tokenRefresher = func(credential azblob.TokenCredential) time.Duration {
		monitor.Info("azure: refreshing azure storage auth token")

		// Get a new token
		if err := spt.Refresh(); err != nil {
			monitor.Error(errors.Internal("azure: unable to refresh service principle token", err))
			panic(err)
		}
		token := spt.Token()
		credential.SetToken(token.AccessToken)

		// Return the expiry time (x minutes before the token expires)
		exp := token.Expires().Sub(time.Now().Add(tokenRefreshBuffer))
		monitor.Info("azure: received new token, valid for %s", exp)
		return exp
	}

	credential := azblob.NewTokenCredential("", tokenRefresher)
	return credential, nil
}

// Write writes the data to a randomly selected storage account sink.
func (m *MultiAccountWriter) Write(key key.Key, val []byte) error {
	containerURL, err := m.getContainerURL()
	if err != nil {
		return err
	}
	return m.WriteToContanier(key, val, containerURL)
}
func (m *MultiAccountWriter) WriteToContanier(key key.Key, val []byte, containerURL *azblob.ContainerURL) error {
	start := time.Now()
	ctx := context.Background()

	blobName := path.Join(m.prefix, string(key))
	blockBlobURL := containerURL.NewBlockBlobURL(blobName)

	_, err := azblob.UploadBufferToBlockBlob(ctx, val, blockBlobURL, m.options)
	if err != nil {
		m.monitor.Count1(ctxTag, "writeerror")
		m.monitor.Info("failed_azure_write: %s", blobName)
		return errors.Internal("azure: unable to write", err)
	}
	m.monitor.Histogram(ctxTag, "writelatency", float64(time.Since(start)))
	return nil
}

func (m *MultiAccountWriter) getContainerURL() (*azblob.ContainerURL, error) {
	if len(m.containerURLs) == 0 {
		return nil, errors.New("azure: no containerURLs initialized")
	}

	if m.chooser != nil {
		return m.chooser.Pick().(*azblob.ContainerURL), nil
	}

	i := rand.Intn(len(m.containerURLs))
	return &m.containerURLs[i], nil
}
