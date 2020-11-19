# Google Pub/Sub sink

This sink implements Google Pub/Sub protocol. It only works for streams, not for compaction like the other sinks. It can can be enabled by adding the following configuration in the `tables` section:

```yaml
tables:
  eventlog:
    streams:
      - pubsub:
          project: my-gcp-project
          topic: my-topic
          filter: "gcs://my-bucket/my-function.lua"
          encoder: json
...
```
