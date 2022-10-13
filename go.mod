module github.com/kelindar/talaria

go 1.18

// should be removed once crphang/orc is up to date with upstream orc
replace github.com/crphang/orc => github.com/tardunge/orc v0.0.7

replace github.com/dgraph-io/badger/v3 v3.2103.1 => github.com/talariadb/badger/v3 v3.2103.1-with-arm-fix

// This may not necessary, but explictly import it in case of some cache in go mod
replace github.com/dgraph-io/ristretto v0.1.0 => github.com/talariadb/ristretto v0.2.0

require (
	cloud.google.com/go/bigquery v1.35.0
	cloud.google.com/go/pubsub v1.3.1
	cloud.google.com/go/storage v1.23.0
	github.com/Azure/azure-sdk-for-go v42.1.0+incompatible
	github.com/Azure/azure-storage-blob-go v0.13.0
	github.com/Azure/go-autorest/autorest v0.11.17
	github.com/Azure/go-autorest/autorest/adal v0.9.11
	github.com/Azure/go-autorest/autorest/azure/auth v0.5.7
	github.com/DataDog/datadog-go v3.7.1+incompatible
	github.com/Knetic/govaluate v3.0.0+incompatible
	github.com/aws/aws-sdk-go v1.30.25
	github.com/crphang/orc v0.0.7
	github.com/dgraph-io/badger/v3 v3.2103.1
	github.com/emitter-io/address v1.0.0
	github.com/fraugster/parquet-go v0.3.0
	github.com/golang/snappy v0.0.3
	github.com/gorilla/mux v1.7.4
	github.com/grab/async v0.0.5
	github.com/hako/durafmt v0.0.0-20191009132224-3f39dc1ed9f4
	github.com/hashicorp/go-multierror v1.1.0
	github.com/hashicorp/memberlist v0.2.2
	github.com/kelindar/binary v1.0.9
	github.com/kelindar/loader v0.0.11
	github.com/kelindar/lua v0.0.7
	github.com/mroth/weightedrand v0.4.1
	github.com/myteksi/hystrix-go v1.1.3
	github.com/samuel/go-thrift v0.0.0-20191111193933-5165175b40af
	github.com/sercand/kuberesolver/v3 v3.0.0
	github.com/stretchr/testify v1.8.0
	github.com/twmb/murmur3 v1.1.3
	go.nhat.io/grpcmock v0.20.0
	golang.org/x/sync v0.0.0-20220601150217-0de741cfad7f
	google.golang.org/api v0.86.0
	google.golang.org/grpc v1.49.0
	google.golang.org/protobuf v1.28.1
	gopkg.in/yaml.v2 v2.4.0
)

require (
	cloud.google.com/go v0.102.1 // indirect
	cloud.google.com/go/compute v1.7.0 // indirect
	cloud.google.com/go/iam v0.3.0 // indirect
	cloud.google.com/go/kms v1.4.0 // indirect
	github.com/Azure/azure-pipeline-go v0.2.3 // indirect
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest/azure/cli v0.4.2 // indirect
	github.com/Azure/go-autorest/autorest/date v0.3.0 // indirect
	github.com/Azure/go-autorest/autorest/to v0.3.0 // indirect
	github.com/Azure/go-autorest/logger v0.2.0 // indirect
	github.com/Azure/go-autorest/tracing v0.6.0 // indirect
	github.com/apache/thrift v0.13.0 // indirect
	github.com/armon/go-metrics v0.3.3 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bool64/shared v0.1.4 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgraph-io/ristretto v0.1.0 // indirect
	github.com/dgryski/go-farm v0.0.0-20200201041132-a6ae2369ad13 // indirect
	github.com/dimchansky/utfbom v1.1.1 // indirect
	github.com/dnaeon/go-vcr v1.0.1 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/form3tech-oss/jwt-go v3.2.2+incompatible // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b // indirect
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/btree v1.0.0 // indirect
	github.com/google/flatbuffers v1.12.0 // indirect
	github.com/google/go-cmp v0.5.8 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.1.0 // indirect
	github.com/googleapis/gax-go/v2 v2.4.0 // indirect
	github.com/googleapis/go-type-adapters v1.0.0 // indirect
	github.com/gopherjs/gopherjs v0.0.0-20200209183636-89e6cbcd0b6d // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.2.0 // indirect
	github.com/hashicorp/go-msgpack v1.1.5 // indirect
	github.com/hashicorp/go-sockaddr v1.0.2 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/iancoleman/orderedmap v0.2.0 // indirect
	github.com/imroc/req v0.3.0 // indirect
	github.com/jmespath/go-jmespath v0.3.0 // indirect
	github.com/klauspost/compress v1.12.3 // indirect
	github.com/mattn/go-ieproxy v0.0.1 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/miekg/dns v1.1.29 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v1.7.1 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.10.0 // indirect
	github.com/prometheus/procfs v0.1.3 // indirect
	github.com/satori/go.uuid v1.2.0 // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/sergi/go-diff v1.2.0 // indirect
	github.com/smartystreets/goconvey v1.6.4 // indirect
	github.com/spf13/afero v1.9.2 // indirect
	github.com/stretchr/objx v0.4.0 // indirect
	github.com/swaggest/assertjson v1.7.0 // indirect
	github.com/yudai/gojsondiff v1.0.0 // indirect
	github.com/yudai/golcs v0.0.0-20170316035057-ecda9a501e82 // indirect
	github.com/yuin/gopher-lua v0.0.0-20191220021717-ab39c6098bdb // indirect
	go.nhat.io/matcher/v2 v2.0.0 // indirect
	go.opencensus.io v0.23.0 // indirect
	golang.org/x/crypto v0.0.0-20211108221036-ceb1ce70b4fa // indirect
	golang.org/x/net v0.0.0-20220624214902-1bab6f366d9e // indirect
	golang.org/x/oauth2 v0.0.0-20220622183110-fd043fe589d2 // indirect
	golang.org/x/sys v0.0.0-20220624220833-87e55d714810 // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/time v0.0.0-20200416051211-89c76fbcd5d1 // indirect
	golang.org/x/xerrors v0.0.0-20220609144429-65e65417b02f // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20220628213854-d9e0b6570c03 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	layeh.com/gopher-luar v1.0.7 // indirect
)
