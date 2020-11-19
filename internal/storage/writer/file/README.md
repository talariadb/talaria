# Google Cloud Storage

This sink supports writing to the local file system. It can can be enabled by adding the following configuration in the `tables` section:

```yaml
tables:
  eventlog:
    compact:                               # enable compaction
      interval: 60                         # compact every 60 seconds
      nameFunc: "s3://bucket/namefunc.lua" # file name function
      file:                                # sink to use
        dir: "/output"                     # the output directory
...
```
