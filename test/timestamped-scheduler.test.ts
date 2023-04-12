import { TransactionOperation, TransactionRegisterKey } from '../lib'
import { Scheduled, TimestampedScheduler } from '../lib/timestamped-scheduler'
import { Database } from '../workloads/transactions'

describe('TimestampedScheduler', () => {
  let database: Database

  beforeEach(() => {
    database = {}
  })

  describe('#register', () => {
    it('increases the timestamp for each transaction', async () => {
      const tss = new TimestampedScheduler(database)
      const tx1 = 1
      const tx2 = 2

      await tss.register(tx1)
      await tss.register(tx2)

      expect(tss.transactions[tx1].timestamp).toBe(1)
      expect(tss.transactions[tx2].timestamp).toBe(2)
    })
  })

  describe('#commit', () => {
    it('removes the transaction from the scheduler state', async () => {
      const txid1 = 1
      const tss = new TimestampedScheduler(database)

      tss.register(txid1)
      await tss.commit(txid1)

      expect(tss.transactions[txid1]).toBeUndefined()
    })

    it('resolves pending transactions', async () => {
      const txid1 = 1
      const txid2 = 2
      const txregister1 = 1
      const tss = new TimestampedScheduler(database)
      const txwriteval = 1

      tss.register(txid1)
      tss.register(txid2)

      await tss.schedule(
        txid1,
        txregister1,
        TransactionOperation.Write,
        txwriteval
      )
      await tss.schedule(txid2, txregister1, TransactionOperation.Read, null)

      let commitStatus: string
      tss.commit(txid2).then((result) => {
        commitStatus = result
      })

      await tss.commit(txid1)

      process.nextTick(() => {
        expect(commitStatus).toBe('committed')
      })
    })
  })
  describe('#abort', () => {
    it('removes the transaction for the scheduler state', async () => {
      const txid1 = 1
      const tss = new TimestampedScheduler(database)

      tss.register(txid1)
      await tss.abort(txid1)

      expect(tss.transactions[txid1]).toBeUndefined()
    })

    it('aborts dependent transactions', async () => {
      const txid1 = 1
      const txid2 = 2
      const txregister1 = 1
      const tss = new TimestampedScheduler(database)
      const tx1writeval = 1

      tss.register(txid1)
      tss.register(txid2)

      await tss.schedule(
        txid1,
        txregister1,
        TransactionOperation.Write,
        tx1writeval
      )
      await tss.schedule(txid2, txregister1, TransactionOperation.Read, null)

      let commitStatus: string
      tss.commit(txid2).then((result) => {
        commitStatus = result
      })

      await tss.abort(txid1)

      process.nextTick(() => {
        expect(commitStatus).toBe('aborted')
      })
    })

    it('resets database state', async () => {
      const txid1 = 1
      const txid2 = 2
      const txregister1 = 1
      const tss = new TimestampedScheduler(database)
      const tx1writeval = 1
      const tx2writeval = 2

      tss.register(txid1)
      tss.register(txid2)

      await tss.schedule(
        txid1,
        txregister1,
        TransactionOperation.Write,
        tx1writeval
      )
      database[txregister1] = tx1writeval
      await tss.commit(txid1)

      await tss.schedule(
        txid2,
        txregister1,
        TransactionOperation.Write,
        tx2writeval
      )
      database[txregister1] = tx2writeval

      await tss.abort(txid2)

      expect(tss.database[txregister1]).toEqual(tx1writeval)
    })

    it('does not reset the database state', async () => {
      const txid1 = 1
      const txid2 = 2
      const txregister1 = 1
      const tss = new TimestampedScheduler(database)
      const tx1writeval = 1
      const tx2writeval = 2

      tss.register(txid1)
      tss.register(txid2)

      await tss.schedule(
        txid1,
        txregister1,
        TransactionOperation.Write,
        tx1writeval
      )
      database[txregister1] = tx1writeval

      await tss.schedule(
        txid2,
        txregister1,
        TransactionOperation.Write,
        tx2writeval
      )
      database[txregister1] = tx2writeval

      await tss.abort(txid1)

      expect(tss.database[txregister1]).toEqual(tx2writeval)
    })
  })

  describe('#schedule', () => {
    let tss: TimestampedScheduler
    const txid1 = 1
    const txid2 = 2
    const txregister1: TransactionRegisterKey = 1

    beforeEach(() => {
      tss = new TimestampedScheduler(database)
    })

    describe('read', () => {
      describe('with no recent write', () => {
        it('returns "run"', async () => {
          await tss.register(txid1)

          const { action } = await tss.schedule(
            txid1,
            txregister1,
            TransactionOperation.Read,
            null
          )

          expect(action).toEqual(Scheduled.Run)
        })

        it('sets the read timestamp for the register', async () => {
          await tss.register(txid1)

          await tss.schedule(
            txid1,
            txregister1,
            TransactionOperation.Read,
            null
          )

          expect(tss.readTimestamps[txregister1]).toEqual(
            tss.transactions[txid1].timestamp
          )
          expect(tss.writeTimestamps[txregister1]).toBeUndefined()
          expect(tss.transactions[txid1].registers).toEqual(
            new Set([txregister1])
          )
        })

        it("adds the register to the transaction's registers", async () => {
          await tss.register(txid1)

          await tss.schedule(
            txid1,
            txregister1,
            TransactionOperation.Read,
            null
          )

          expect(tss.transactions[txid1].registers).toEqual(
            new Set([txregister1])
          )
        })
      })

      describe('with an older write', () => {
        it('adds the writing transaction to the dependencies list', async () => {
          const tx1writeval = 1
          await tss.register(txid1)
          await tss.register(txid2)

          await tss.schedule(
            txid1,
            txregister1,
            TransactionOperation.Write,
            tx1writeval
          )
          await tss.schedule(
            txid2,
            txregister1,
            TransactionOperation.Read,
            null
          )

          expect(tss.transactions[txid2].dependsOn).toEqual([txid1])
        })

        it("adds the register to the transaction's registers", async () => {
          await tss.register(txid1)

          await tss.schedule(
            txid1,
            txregister1,
            TransactionOperation.Read,
            null
          )

          expect(tss.transactions[txid1].registers).toEqual(
            new Set([txregister1])
          )
        })
      })

      describe('with a more recent read', () => {
        it('does not set the read timestamp for the register', async () => {
          await tss.register(txid1)
          await tss.register(txid2)

          await tss.schedule(
            txid2,
            txregister1,
            TransactionOperation.Read,
            null
          )
          await tss.schedule(
            txid1,
            txregister1,
            TransactionOperation.Read,
            null
          )

          expect(tss.readTimestamps[txregister1]).toEqual(
            tss.transactions[txid2].timestamp
          )
        })
      })

      describe('with a more recent write', () => {
        it('returns "abort"', async () => {
          const tx2writeval = 1
          await tss.register(txid1)
          await tss.register(txid2)

          await tss.schedule(
            txid2,
            txregister1,
            TransactionOperation.Write,
            tx2writeval
          )
          const { action } = await tss.schedule(
            txid1,
            txregister1,
            TransactionOperation.Read,
            null
          )

          expect(action).toEqual(Scheduled.Abort)
        })

        it('does not set the read timestamp for the register', async () => {
          const tx2writeval = 1
          await tss.register(txid1)
          await tss.register(txid2)

          await tss.schedule(
            txid2,
            txregister1,
            TransactionOperation.Write,
            tx2writeval
          )
          await tss.schedule(
            txid1,
            txregister1,
            TransactionOperation.Read,
            null
          )

          expect(tss.readTimestamps[txregister1]).toBeUndefined()
        })

        it("does not add the register to the transaction's registers", async () => {
          const tx2writeval = 1
          await tss.register(txid1)
          await tss.register(txid2)

          await tss.schedule(
            txid2,
            txregister1,
            TransactionOperation.Write,
            tx2writeval
          )
          await tss.schedule(
            txid1,
            txregister1,
            TransactionOperation.Read,
            null
          )

          expect(tss.transactions[txid1].registers).toEqual(new Set([]))
        })
      })
    })

    describe('write', () => {
      describe('without a more recent read or write', () => {
        it('returns "run"', async () => {
          const tx1writeval = 1
          await tss.register(txid1)

          const { action } = await tss.schedule(
            txid1,
            txregister1,
            TransactionOperation.Write,
            tx1writeval
          )

          expect(action).toEqual(Scheduled.Run)
        })

        it('sets the write timestamp for the register', async () => {
          const tx1writeval = 1
          await tss.register(txid1)

          await tss.schedule(
            txid1,
            txregister1,
            TransactionOperation.Write,
            tx1writeval
          )

          expect(tss.writeTimestamps[txregister1]).toEqual(
            tss.transactions[txid1].timestamp
          )
        })

        it("adds the register to the transaction's registers", async () => {
          const tx1writeval = 1
          await tss.register(txid1)

          await tss.schedule(
            txid1,
            txregister1,
            TransactionOperation.Write,
            tx1writeval
          )

          expect(tss.transactions[txid1].registers).toEqual(
            new Set([txregister1])
          )
        })
      })

      describe('with a more recent read', () => {
        it('returns "abort"', async () => {
          const tx2writeval = 1
          await tss.register(txid1)
          await tss.register(txid2)

          await tss.schedule(
            txid2,
            txregister1,
            TransactionOperation.Read,
            null
          )
          const { action } = await tss.schedule(
            txid1,
            txregister1,
            TransactionOperation.Write,
            tx2writeval
          )

          expect(action).toEqual(Scheduled.Abort)
        })

        it('does not set the timestamp for the register', async () => {
          const tx1writeval = 1
          await tss.register(txid1)
          await tss.register(txid2)

          await tss.schedule(
            txid2,
            txregister1,
            TransactionOperation.Read,
            null
          )
          await tss.schedule(
            txid1,
            txregister1,
            TransactionOperation.Write,
            tx1writeval
          )

          expect(tss.writeTimestamps[txregister1]).toBeUndefined()
        })

        it("does not add the register to the transaction's registers", async () => {
          const tx1writeval = 1
          await tss.register(txid1)
          await tss.register(txid2)

          await tss.schedule(
            txid2,
            txregister1,
            TransactionOperation.Read,
            null
          )
          await tss.schedule(
            txid1,
            txregister1,
            TransactionOperation.Write,
            tx1writeval
          )

          expect(tss.transactions[txid1].registers).toEqual(new Set([]))
        })
      })

      describe('with a more recent write', () => {
        it('returns skip', async () => {
          const tx1writeval = 1
          const tx2writeval = 2
          await tss.register(txid1)
          await tss.register(txid2)

          await tss.schedule(
            txid2,
            txregister1,
            TransactionOperation.Write,
            tx2writeval
          )
          const { action } = await tss.schedule(
            txid1,
            txregister1,
            TransactionOperation.Write,
            tx1writeval
          )

          expect(action).toEqual(Scheduled.Skip)
        })

        it('does not set the timestamp for the register', async () => {
          const tx1writeval = 1
          const tx2writeval = 2
          await tss.register(txid1)
          await tss.register(txid2)

          await tss.schedule(
            txid2,
            txregister1,
            TransactionOperation.Write,
            tx2writeval
          )
          await tss.schedule(
            txid1,
            txregister1,
            TransactionOperation.Write,
            tx1writeval
          )

          expect(tss.writeTimestamps[txregister1]).toEqual(
            tss.transactions[txid2].timestamp
          )
        })

        it("does not add the register to the transaction's registers", async () => {
          const tx1writeval = 1
          const tx2writeval = 2
          await tss.register(txid1)
          await tss.register(txid2)

          await tss.schedule(
            txid2,
            txregister1,
            TransactionOperation.Write,
            tx2writeval
          )
          await tss.schedule(
            txid1,
            txregister1,
            TransactionOperation.Write,
            tx1writeval
          )

          expect(tss.transactions[txid1].registers).toEqual(new Set([]))
        })
      })
    })
  })
})
