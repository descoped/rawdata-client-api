# The copy tool

`rawdata-copy` copies topics from one store to another, keeping ids and order: a backup, or a move from one provider
or cloud to another, for example from `gcs` to `s3`, or to `postgres` on another cloud. When a target topic has
messages, the copy continues after its last id, so a copy that stopped can be run again.

It is distributed as `rawdata-copy-<version>.zip` (the jar and `lib/` with every provider) on GitHub releases, not
on Maven Central. To build it: `./mvnw -pl rawdata-copy-util -am package -DskipTests`, which writes
`rawdata-copy-util/target/rawdata-copy-<version>.zip`.

## Running it

```properties
# copy.properties: a source.* and a target.* provider configuration
source.rawdata.client.provider=gcs
source.gcs.bucket-name=my-bucket
source.gcs.credential-provider=application-default
source.local-temp-folder=/var/tmp/rawdata-copy
target.rawdata.client.provider=postgres
target.postgres.driver.host=db.example.com
target.postgres.driver.user=rdc
target.postgres.driver.password=secret
target.postgres.driver.database=rdc
```

```sh
unzip rawdata-copy-<version>.zip
java -jar rawdata-copy-<version>/rawdata-copy-util-<version>.jar --config copy.properties --metadata topic-a topic-b:renamed
java -jar rawdata-copy-<version>/rawdata-copy-util-<version>.jar --help
```

It runs with `java -jar` (the class path), or as a module: `java -p rawdata-copy-util-<version>.jar:lib -m
io.descoped.rawdata.copy/io.descoped.rawdata.copy.RawdataCopyTool`. Exit status 0: copied; 1: a copy failed, run it
again to continue; 2: wrong arguments or configuration, or a store that cannot be opened, nothing copied.

## Moving a running system to another store or cloud

1. Copy every topic while the old system runs (`--metadata` to bring checkpoints and other metadata along).
2. Stop the producers on the old store.
3. Run the same copy again: it copies only what came since.
4. Point the producers, and then the consumers, at the new store.

Nothing may write to a target topic before the last copy of step 3. A message written there first would move the
topic's last id past the messages still to be copied, and the copy, which continues after the last id, would skip
them.

## Options

```
rawdata-copy --config <file> [options] <topic>[:<target-topic>]...

  --config <file>          properties with source.* and target.* provider configurations (required)
  --metadata               also copy each topic's metadata entries, overwriting target entries with the same key
  --from-id <uuid>         start at this id (inclusive) instead of the beginning
  --from-time <time>       start at this time: an ISO-8601 instant or epoch milliseconds
  --batch-size <n>         messages per publish to the target (default 1000)
  --idle-timeout-ms <ms>   stop when the source has had nothing new for this long (default 1000)
```

A start point earlier than the target's last id is ignored: the copy always continues after what the target has,
because a target topic only grows at its end. Each batch is one publish, so with a file-based target
(`filesystem`, `gcs`, `s3`) the batch size is the number of messages per file.
