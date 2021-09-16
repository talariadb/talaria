module github.com/kelindar/talaria

go 1.16

// should be removed once crphang/orc is up to date with upstream orc
replace github.com/crphang/orc => github.com/tardunge/orc v0.0.7

require (
	cloud.google.com/go v0.57.0 // indirect
	cloud.google.com/go/bigquery v1.7.0
	cloud.google.com/go/pubsub v1.3.1
	cloud.google.com/go/storage v1.7.0
	github.com/Azure/azure-sdk-for-go v42.1.0+incompatible
	github.com/Azure/azure-storage-blob-go v0.13.0
	github.com/Azure/go-autorest/autorest/azure/auth v0.5.7
	github.com/Azure/go-autorest/autorest/to v0.3.0 // indirect
	github.com/DataDog/datadog-go v3.7.1+incompatible
	github.com/Knetic/govaluate v3.0.0+incompatible
	github.com/armon/go-metrics v0.3.3 // indirect
	github.com/aws/aws-sdk-go v1.30.25
	github.com/crphang/orc v0.0.7
	github.com/dgraph-io/badger/v3 v3.2103.1
	github.com/dgryski/go-farm v0.0.0-20200201041132-a6ae2369ad13 // indirect
	github.com/dnaeon/go-vcr v1.0.1 // indirect
	github.com/emitter-io/address v1.0.0
	github.com/fraugster/parquet-go v0.3.0
	github.com/golang/snappy v0.0.3
	github.com/gopherjs/gopherjs v0.0.0-20200209183636-89e6cbcd0b6d // indirect
	github.com/gorilla/mux v1.7.4
	github.com/grab/async v0.0.5
	github.com/hako/durafmt v0.0.0-20191009132224-3f39dc1ed9f4
	github.com/hashicorp/go-immutable-radix v1.2.0 // indirect
	github.com/hashicorp/go-msgpack v1.1.5 // indirect
	github.com/hashicorp/go-multierror v1.1.0
	github.com/hashicorp/go-sockaddr v1.0.2 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/hashicorp/memberlist v0.2.2
	github.com/imroc/req v0.3.0 // indirect
	github.com/kelindar/binary v1.0.9
	github.com/kelindar/loader v0.0.11
	github.com/kelindar/lua v0.0.7
	github.com/miekg/dns v1.1.29 // indirect
	github.com/mroth/weightedrand v0.4.1
	github.com/myteksi/hystrix-go v1.1.3
	github.com/planetscale/vtprotobuf v0.2.0
	github.com/samuel/go-thrift v0.0.0-20191111193933-5165175b40af
	github.com/satori/go.uuid v1.2.0 // indirect
	github.com/sercand/kuberesolver/v3 v3.0.0
	github.com/smartystreets/goconvey v1.6.4 // indirect
	github.com/stretchr/objx v0.2.0 // indirect
	github.com/stretchr/testify v1.5.1
	github.com/twmb/murmur3 v1.1.3
	github.com/yuin/gopher-lua v0.0.0-20191220021717-ab39c6098bdb // indirect
	golang.org/x/net v0.0.0-20210326060303-6b1517762897 // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/time v0.0.0-20200416051211-89c76fbcd5d1 // indirect
	google.golang.org/api v0.24.0
	google.golang.org/grpc v1.40.0
	google.golang.org/protobuf v1.26.0
	gopkg.in/yaml.v2 v2.2.8
)
