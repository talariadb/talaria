# Talaria

This sink implements sending data to a second Talaria. It can be used when there is one Talaria for **event ingestion**, and needs to write data to a second Talaria, which is used **purely for querying data.**

This sink can be enabled by adding the following configuration in the `tables` section:

```yaml
tables:
  eventlog:
    compact:                               # enable compaction
      interval: 60                         # compact every 60 seconds
      nameFunc: "s3://bucket/namefunc.lua" # file name function
      talaria:                             # sink to Talaria
        endpoint: "127.0.0.1:8043"         # Talaria endpoint to write data to
        timeout: 5                         # Timeout for requests to Talaria
        maxConcurrent: 10                  # Number of concurrent requests to Talaria
        errorThreshold: 50                 # Percentage of errors before no more requests are sent
...
```
