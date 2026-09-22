module io.dbx.kafka.plugin

go 1.25.0

require (
	github.com/aws/aws-msk-iam-sasl-signer-go v1.0.4
	github.com/aws/aws-sdk-go-v2/config v1.33.3
	github.com/aws/aws-sdk-go-v2/credentials v1.20.5
	github.com/aws/aws-sdk-go-v2/service/glue v1.158.0
	github.com/aws/smithy-go v1.28.1
	github.com/go-zookeeper/zk v1.0.4
	github.com/google/jsonschema-go v0.4.2
	github.com/google/uuid v1.6.0
	github.com/jcmturner/gokrb5/v8 v8.4.4
	github.com/klauspost/compress v1.20.0
	github.com/linkedin/goavro/v2 v2.15.0
	github.com/pierrec/lz4/v4 v4.1.26
	github.com/t8y2/dbx/plugins/sdk/go/dbx-plugin-sdk v0.0.0-00010101000000-000000000000
	github.com/twmb/franz-go v1.21.6
	github.com/twmb/franz-go/pkg/kadm v1.18.0
	github.com/twmb/franz-go/pkg/kmsg v1.13.1
	github.com/twmb/franz-go/pkg/sasl/kerberos v1.1.0
	golang.org/x/net v0.52.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/aws/aws-sdk-go-v2 v1.47.0 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.20.0 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.3 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.3 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.5.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.19 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.14.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.10.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.38.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.43.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.51.0 // indirect
	github.com/golang/snappy v0.0.1 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	golang.org/x/crypto v0.50.0 // indirect
)

// SDK vendored 以便独立 checkout 构建（shared/sdk/go/dbx-plugin-sdk，拆分基线
// 见 docs/REPOSITORY_SPLIT.zh-CN.md；不在公网 module proxy 上，unknown revision）。
// `dbx-plugin package` 打包时 CLI 会通过 DBX_PLUGIN_SDK_ROOT + go.work 注入
// 自己的解析路径，此 replace 不影响打包。
replace github.com/t8y2/dbx/plugins/sdk/go/dbx-plugin-sdk => ../shared/sdk/go/dbx-plugin-sdk
