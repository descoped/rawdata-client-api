# The `gcs` provider

Topics as Avro files in a Google Cloud Storage bucket. Artifact `rawdata-client-provider-gcs`, module
`io.descoped.rawdata.provider.gcs`. It is the [`filesystem`](filesystem.md) engine on a bucket: the same segments,
layout and order. It talks to GCS's JSON API with the JDK's HTTP client and does Google's OAuth 2.0 itself, so it
needs no Google libraries: its only dependency besides the Avro engine is Jackson.

## Configuration

| Key | Default | |
|---|---|---|
| `rawdata.client.provider` | | `gcs` |
| `gcs.bucket-name` | | required |
| `gcs.prefix` | none | a folder in the bucket for the topics, for example `rawdata/` |
| `gcs.credentials` | `application-default` | `application-default`, `service-account` or `none` |
| `gcs.service-account.key-file` | | the JSON key, for `service-account` |
| `gcs.endpoint` | `https://storage.googleapis.com` | another endpoint, for example an emulator's `http://localhost:4443` |
| `gcs.poll-interval-ms` | `1000` | how often a waiting consumer looks for new messages |

### Credentials

`application-default` finds Application Default Credentials in Google's order:

1. the file named by `GOOGLE_APPLICATION_CREDENTIALS`: a service account key, or a gcloud user's credentials;
2. gcloud's `application_default_credentials.json` (after `gcloud auth application-default login`), in
   `$CLOUDSDK_CONFIG`, `~/.config/gcloud`, or `%APPDATA%\gcloud` on Windows;
3. the metadata server of a Google Cloud runtime (GCE, GKE, Cloud Run, Cloud Functions), if it answers
   (`GCE_METADATA_HOST` overrides its address).

`service-account` reads the key in `gcs.service-account.key-file`: a JWT signed with the key (RS256) is exchanged
for a token at Google's token endpoint. A gcloud user's refresh token is exchanged the same way, and the metadata
server gives tokens for the runtime's service account. Tokens have the `devstorage.read_write` scope and are kept
until five minutes before they expire.

Workload identity federation (`external_account` files) is not supported yet; such a file is refused with a message
saying so. The account needs to create, read, list and delete objects in the bucket, for example
`roles/storage.objectUser`.

## Objects

```
gs://<bucket>/<prefix><topic>/segments/0000000000000000000.avro
gs://<bucket>/<prefix><topic>/metadata/k<hex of the key>
```

A segment is uploaded in one request with the precondition that no object has its name (`ifGenerationMatch=0`). If
the name is taken, another process wrote the topic, and the publish throws `RawdataWriterConflictException` (see
[one writer per topic](../contract.md#one-writer-per-topic)). If an upload succeeded but its response was lost, the
client's retry fails the precondition; the store then reads the object back, and if it holds its own bytes the
segment counts as created. Metadata entries are written with a precondition on the generation they replace, so the
client can retry them. A metadata key is at most (1024 − the prefix, topic and `/metadata/k`) / 2 bytes, about 500; a prefix is therefore at most 659 bytes, so that keys of 127 bytes always fit.

GCS answers a missing object and a missing bucket with the same 404. The first time a client sees one, it lists the
bucket once, so a wrong bucket name fails instead of looking like an empty topic.

Every request is retried on an I/O error, 408, 429 and 5xx, with exponential backoff and jitter. That is safe:
reads and deletes are idempotent, and every upload has a generation precondition. Getting a token is retried the same
way, unless Google refuses it (a 4xx such as `invalid_grant`). A 401 from GCS drops the kept token and tries once
more with a new one.

Messages are visible and durable when `publish` returns, one object per publish: publish in batches where you can.

## Dependencies and the module path

The provider depends on the Avro engine and Jackson, nothing else, and is a named module: put it on the module path
or the class path.

The tests run against [fake-gcs-server](https://github.com/fsouza/fake-gcs-server) in Docker, and the credential
sources against a local server playing Google's token endpoint and metadata server. A real bucket with real
credentials is checked by hand before a release.
