# Google Cloud Storage

This sink implements Google Cloud Storage protocol. It can can be enabled by adding the following configuration in the `tables` section:

```yaml
tables:
  eventlog:
    compact:                               # enable compaction
      interval: 60                         # compact every 60 seconds
      nameFunc: "s3://bucket/namefunc.lua" # file name function
      gcs:                                  # sink to use
        bucket: "bucket"                   # the bucket to use
        prefix: "dir1/"                    # (optional) prefix to add
...
```
