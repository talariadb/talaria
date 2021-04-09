# Azure Blob Storage

This sink implements Azure Blob Storage protocol. It can be enabled by adding the following configuration in the `tables` section:

```yaml
tables:
  eventlog:
    compact:                               # enable compaction
      interval: 60                         # compact every 60 seconds
      nameFunc: "s3://bucket/namefunc.lua" # file name function
      azure:                               # sink to use
        container: "container-id-1"        # the container ID
        prefix: ""                         # (optional) prefix to add
...
```
This sink can write to different Storage Accounts randomly each time, if multiple storage accounts are configured
```yaml
tables:
  eventlog:
    compact:                               # enable compaction
      interval: 60                         # compact every 60 seconds
      nameFunc: "s3://bucket/namefunc.lua" # file name function
      azure:                               # sink to use
        container: "container-id-1"        # the container ID
        prefix: ""                         # (optional) prefix to add
        defaultBlobServiceURL: ""          # (optional) if not set, use default
        storageAccounts:                   # (optional) if not set, use single storage writer
          - "storage-account-id-0"
          - "storage-account-id-1"
...
```
