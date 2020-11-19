# Amazon S3 sink, Minio, Digital Ocean Spaces

This sink implements Amazon S3 protocol and can be enabled for Digital Ocean Spaces and Minio using a custom `endpoint`.

This sink can be enabled by adding the following configuration in the `tables` section:

```yaml
tables:
  eventlog:
    compact:                               # enable compaction
      interval: 60                         # compact every 60 seconds
      nameFunc: "s3://bucket/namefunc.lua" # file name function
      s3:                                  # sink to Amazon S3
        region: "ap-southeast-1"           # the region to use
        bucket: "bucket"                   # the bucket to use
        prefix: "dir1/"                    # (optional) prefix to add
        endpoint: "http://127.0.0.1"       # (optional) custom endpoint to use
        sse: ""                            # (optional) server-side encryption
        accessKey: ""                      # (optional) static access key to override
        secretKey: ""                      # (optional) static secret key to override
        concurrency: 32                    # (optional) upload concurrency, default=NUM_CPU
...
```
