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
	"github.com/Azure/go-autorest/autorest/adal"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/Azure/go-autorest/autorest/azure/auth"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/key"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/errors"
	"github.com/kelindar/talaria/internal/storage/writer/base"
	"github.com/mroth/weightedrand"
)

const (
	ctxTag                = "azure"
	tokenRefreshBuffer    = 2 * time.Minute
	defaultBlobServiceURL = "https://%s.blob.core.windows.net"
	defaultResourceID     = "https://storage.azure.com/"
)

// Writer represents a writer for Microsoft Azure.
type Writer struct {
	*base.Writer
	monitor   monitor.Monitor
	prefix    string
	container *storage.Container
}

// MultiAccountWriter represents a writer for Microsoft Azure with multiple storage accounts.
type MultiAccountWriter struct {
	*base.Writer
	monitor        monitor.Monitor
	blobServiceURL string
	prefix         string
	containerURLs  []azblob.ContainerURL
	options        azblob.UploadToBlockBlobOptions
	chooser        *weightedrand.Chooser
}

// New creates a new writer.
func New(container, prefix, filter, encoding string, monitor monitor.Monitor) (*Writer, error) {

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
	// Load Encoder and Filter
	baseWriter, err := base.New(filter, encoding, monitor)
	if err != nil {
		return nil, err
	}

	// Create a new storage client
	client, err := storage.NewClient(accountName, accountKey, serviceBaseURL, apiVersion, true)
	if err != nil {
		return nil, errors.Internal("azure: unable to create a client", err)
	}

	svc := client.GetBlobService()
	ref := svc.GetContainerReference(container)
	return &Writer{
		Writer:    baseWriter,
		monitor:   monitor,
		prefix:    prefix,
		container: ref,
	}, nil
}

// Write writes the data to the sink.
func (w *Writer) Write(key key.Key, blocks []block.Block) error {
	if w.container == nil {
		return errors.New("azure: unable to obtain a container reference")
	}

	ref := w.container.GetBlobReference(path.Join(w.prefix, string(key)))
	if err := ref.PutAppendBlob(nil); err != nil {
		return errors.Internal("azure: unable to write", err)
	}

	buffer, err := w.Writer.Encode(blocks)
	if err != nil {
		return errors.Internal("encoder: unable to encode blocks to bytes ", err)
	}

	if err := ref.AppendBlock(buffer, nil); err != nil {
		return errors.Internal("azure: unable to write", err)
	}
	return nil
}

// NewMultiAccountWriter creates a new MultiAccountWriter.
func NewMultiAccountWriter(monitor monitor.Monitor, filter, encoding, blobServiceURL, container, prefix string, storageAccount []string, weights []uint, parallelism uint16, blockSize int64) (*MultiAccountWriter, error) {
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
		monitor.Info(fmt.Sprintf("azure: new azure storage pipeline created for  %s", u))

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
			monitor.Info(fmt.Sprintf("azure: writer weights for  %v set to %d", containerURLs[i], w))
		}
		chooser, err = weightedrand.NewChooser(choices...)
		if err != nil {
			return nil, err
		}
	}

	// Load Encoder and Filter
	baseWriter, err := base.New(filter, encoding, monitor)
	if err != nil {
		return nil, err
	}

	return &MultiAccountWriter{
		Writer:        baseWriter,
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

	spt, err := getServicePrincipalToken(monitor)

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
func (m *MultiAccountWriter) Write(key key.Key, blocks []block.Block) error {
	containerURL, err := m.getContainerURL()
	if err != nil {
		return err
	}
	buffer, err := m.Writer.Encode(blocks)
	if err != nil {
		return errors.Internal("encoder: unable to encode blocks to bytes ", err)
	}

	return m.WriteToContanier(key, buffer, containerURL)
}
func (m *MultiAccountWriter) WriteToContanier(key key.Key, buffer []byte, containerURL *azblob.ContainerURL) error {
	start := time.Now()
	ctx := context.Background()

	blobName := path.Join(m.prefix, string(key))
	blockBlobURL := containerURL.NewBlockBlobURL(blobName)

	_, err := azblob.UploadBufferToBlockBlob(ctx, buffer, blockBlobURL, m.options)
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

func getServicePrincipalToken(monitor monitor.Monitor) (*adal.ServicePrincipalToken, error) {
	if settings, err := auth.GetSettingsFromEnvironment(); err == nil {
		if cc, err := settings.GetClientCredentials(); err == nil {
			monitor.Info("azure: acquired Credentials from Environment")
			return cc.ServicePrincipalToken()
		}
	}

	spt, err := adal.NewServicePrincipalTokenFromManagedIdentity(azure.PublicCloud.ResourceIdentifiers.Storage, &adal.ManagedIdentityOptions{})
	if err != nil {
		monitor.Warning(errors.Internal("azure: unable to retrieve Credentials", err))
		return spt, err
	}

	monitor.Info("azure: acquired Credentials")
	return spt, err

}
