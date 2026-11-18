# The `gcs` provider

Topics as Avro files in a Google Cloud Storage bucket. Artifact `rawdata-client-provider-gcs`, automatic module
`io.descoped.rawdata.provider.gcs`. It is the [`filesystem`](filesystem.md) engine on a bucket: the same segments,
layout and order.

## Configuration

| Key | Default | |
|---|---|---|
| `rawdata.client.provider` | | `gcs` |
| `gcs.bucket-name` | | required |
| `gcs.prefix` | none | a folder in the bucket for the topics, for example `rawdata/` |
| `gcs.credentials` | `application-default` | `application-default`, `service-account` or `none` |
| `gcs.service-account.key-file` | | the JSON key, for `service-account` |
| `gcs.project-id` | from the environment | |
| `gcs.endpoint` | Google's | another endpoint, for example an emulator's `http://localhost:4443` |
| `gcs.poll-interval-ms` | `1000` | how often a waiting consumer looks for new messages |

`application-default` uses Application Default Credentials: a service account attached to the workload on Google
Cloud, or `gcloud auth application-default login` on a workstation. The account needs to create, read, list and
delete objects in the bucket (for example `roles/storage.objectUser`).

## Objects

```
gs://<bucket>/<prefix><topic>/segments/0000000000000000000.avro
gs://<bucket>/<prefix><topic>/metadata/k<hex of the key>
```

A segment is uploaded in one request with the precondition that no object has its name (`ifGenerationMatch=0`), so
two writers never both create it; the one that loses reads the new last segment and tries the next number. If an
upload succeeded but its response was lost, the client's retry fails the precondition; the store then reads the
object back, and if it holds its own bytes the segment counts as created. Metadata entries are written with a
precondition on the generation they replace, so the client can retry them. A metadata key is at most about 500 bytes
(an object name is at most 1024 bytes with the prefix and topic).

Messages are visible and durable when `publish` returns, one object per publish: publish in batches where you can.

## Dependencies and the module path

The provider uses the JSON API over HTTP with the JDK's HTTP client. Left out of google-cloud-storage, because that
path never loads them: Google's gRPC transport, Cloud Monitoring and the OpenTelemetry SDK (metrics export), and
legacy jars such as Apache HttpClient 4 (end of life); the build fails if a Google update brings one back. The
Google Cloud libraries split packages among themselves, so they cannot be modules: put them on the class path and
the rawdata jars on the module path (see [getting started](../getting-started.md#the-module-path)), or everything on
the class path.

The tests run against [fake-gcs-server](https://github.com/fsouza/fake-gcs-server) in Docker.
