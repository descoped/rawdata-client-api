# Security

## Supported versions

Only the latest release gets security fixes. 3.0.0 is in progress; until it is released, fixes go to `master`.

## Reporting a vulnerability

Please report it privately through GitHub: **Security → Report a vulnerability** on
[descoped/rawdata-client-project](https://github.com/descoped/rawdata-client-project/security/advisories/new). Do
not open a public issue. Include what is affected, how to reproduce it, and the impact you see.

## What the client guards against

- Topic names are validated in every provider and never become part of SQL, file paths or object names in a way that
  could reach outside the topic: SQL takes them as bind parameters, and file and object names are built from the
  validated name only.
- Metadata keys never reach a file or object name as given; they are hex-encoded.
- Messages and metadata are copied in and out, so a caller cannot change stored data through an array it holds.

Credentials (database passwords, GCS service account keys, AWS keys) come from the configuration you pass in; the client does
not log them. Keep configuration files with credentials out of version control.
