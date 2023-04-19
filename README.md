# About

This repo host the solutions to the distributed systems that
[Fly.io](https://fly.io/blog/gossip-glomers/) created. The 6 gossip glomers are
covered in the repo:

- An [echo server](./workloads/echo.ts) https://fly.io/dist-sys/1/
- A totally available [unique id generation service](./workloads/unique-ids.ts)
  https://fly.io/dist-sys/2/
- [Message broadcasting](./workloads/broadcast.ts) https://fly.io/dist-sys/3a/
- A [grow only counter](./workloads/grow-only-counter.ts)
  https://fly.io/dist-sys/4/
- A [kafka-like log](./workloads/kafka-log.ts) https://fly.io/dist-sys/5a/
- A [totally available database](./workloads/transactions.ts)
  https://fly.io/dist-sys/6a/

## Notes

 - Some of the Fly.io exercies are split into several steps. The implementation
   in this repo is the combination of all those steps (or the reflection of the
   last step)
 - I solved the challenges in Typescript and not in Go
