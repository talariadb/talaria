# Google Big Query

This sink implements Google Big Query protocol. It can can be enabled by adding the following configuration in the `tables` section:

```yaml
tables:
  eventlog:
    compact:                               # enable compaction
      interval: 60                         # compact every 60 seconds
      nameFunc: "s3://bucket/namefunc.lua" # file name function
      bigquery:                            # sink to use
        project: "project-id-1"            # project ID
        dataset: "mydataset"               # big query dataset ID
        table: "mytable"                   # big query table ID
...
```
