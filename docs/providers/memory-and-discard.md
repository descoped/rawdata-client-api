# The `memory` and `discard` providers

Both are in `rawdata-client-api`, need no configuration besides `rawdata.client.provider`, and are meant for tests.

- **`memory`** keeps topics and metadata in the client's memory. They are gone when the client closes, and two
  clients do not share them, so the contract tests with a second client on the same store do not apply to it. It
  keeps the rest of the contract, so tests written against it hold for the other providers.
- **`discard`** accepts every publish and keeps nothing: every topic is empty, every consumer waits, and metadata is
  never kept. Topic names and closing are checked as in the other providers. For measuring a producer, or for
  running a pipeline without storing its output.
