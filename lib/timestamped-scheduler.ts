import { log, TransactionOperation, TransactionRegisterKey } from '../lib'
import {
  Database,
  DBRegisterValue,
  TransactionId,
} from '../workloads/transactions'

export type TransactionTimestamp = number
export enum Scheduled {
  Run = 'run',
  Abort = 'abort',
  Skip = 'skip',
}

export enum AbortReason {
  ReadUncommited = 'read_uncommited',
  RepeatableRead = 'repeatable_read',
}

export type ScheduleAbort = {
  action: Scheduled.Abort
  reason: AbortReason
  conflictsWith: TransactionId
}

const UNUSED_PAST_TIMESTAMP = -1

class Deferred {
  public promise: Promise<void>
  private resolver: () => void
  private rejecter: () => void

  constructor() {
    /**
     * Defining this two noops here to
     * satisfy the TS compiler
     */
    this.resolver = () => void 0
    this.rejecter = () => void 0
    this.promise = new Promise((resolve, reject) => {
      this.resolver = resolve
      this.rejecter = reject
    })

    // Attach a default handler so we don't have
    // and UNHANDLED_REJECTION_ERROR
    this.promise.catch(() => undefined)
  }

  resolve(): void {
    this.resolver()
  }

  reject(): void {
    this.rejecter()
  }
}

export class TimestampedScheduler {
  // This is an implementation of https://en.wikipedia.org/wiki/Timestamp-based_concurrency_control
  public transactions: Record<
    TransactionId,
    {
      timestamp: TransactionTimestamp
      deferred: Deferred
      dependsOn: Array<TransactionId>
      overwrites: Array<
        [TransactionRegisterKey, DBRegisterValue, TransactionTimestamp]
      >
    }
  >
  public timestampToTransactions: Record<TransactionTimestamp, TransactionId>
  public nextTimestamp: TransactionTimestamp
  public writeTimestamps: Record<TransactionRegisterKey, TransactionTimestamp>
  public readTimestamps: Record<TransactionRegisterKey, TransactionTimestamp>
  public database: Database

  constructor(database: Database) {
    this.timestampToTransactions = {}
    this.writeTimestamps = {}
    this.readTimestamps = {}
    this.nextTimestamp = 1
    this.transactions = {}
    this.database = database
  }

  async register(transactionId: TransactionId): Promise<void> {
    this.timestampToTransactions[this.nextTimestamp] = transactionId
    this.transactions[transactionId] = {
      timestamp: this.nextTimestamp,
      deferred: new Deferred(),
      dependsOn: [],
      overwrites: [],
    }
    this.nextTimestamp += 1

    return
  }

  async abort(transactionId: TransactionId): Promise<void> {
    const txsToRevert = [transactionId]

    while (txsToRevert.length > 0) {
      const txToRevert = txsToRevert.shift() as number
      const { overwrites } = this.transactions[txToRevert]

      overwrites.forEach(([register, value, timestamp]) => {
        if (
          this.writeTimestamps[register] ===
          this.transactions[txToRevert].timestamp
        ) {
          this.database[register] = value
          this.writeTimestamps[register] = timestamp

          if (this.timestampToTransactions[timestamp]) {
            /**
             * For each reverted transaction we have to consider reverting the
             * transaction that made the latest change prior to the one we are
             * reverting for this transaction. Without doing this we can leave
             * the database in an incorrect state. Take the following example:
             *
             *
             *                W1(X)                             W1(X)
             *   T1  ─────────────────────────────────────────────────────────
             *
             *
             *
             *                          R2(X)        W2(X)              COMMIT
             *   T2  ─────────────────────────────────────────────────────────
             *
             * T1 will abort on the second write. T2 will abort on commit
             * (dependency with T1 which already aborted). T1 won't revert it's
             * write because it's timestamp is different to the timestamp of
             * the register. When T2 reverts it will revert the value of X to
             * that of the first write of T1 which is incorrect too.
             */
            txsToRevert.push(this.timestampToTransactions[timestamp])
          }
        }
      })
    }
    this.clearRegisters(transactionId)

    this.transactions[transactionId].deferred.reject()
    // Can't delete the transaction because it can be in the "dependsOn" of
    // other concurrent transactions
    // delete this.transactions[transactionId]
  }

  async commit(transactionId: TransactionId): Promise<'committed' | 'aborted'> {
    log(`commit ${transactionId} ${this.transactions[transactionId].dependsOn}`)
    const promises = this.transactions[transactionId].dependsOn.map((txid) => {
      if (this.transactions[txid] === undefined) {
        throw new Error(`unexpected transaction ${txid} not found`)
      }

      if (txid === transactionId) {
        // Avoid self loops
        return
      }

      return this.transactions[txid].deferred.promise
    })

    try {
      await Promise.all(promises)
      this.transactions[transactionId].deferred.resolve()

      /**
       * We have to clear the timestamps for the registers read and written by
       * this transaction to avoid creating fake dependencies with other
       * transactions.
       *
       * After this transaction has commited there can't be a conflict with the
       * read and write operations so it's not necessary to keep the timestamps
       * for the operations this transaction dit.
       */
      this.clearRegisters(transactionId)

      /**
       * Once a transaction has commited its changes are "persisted" so
       * we won't undo its changes. Therefore we clear the "overwrites".
       *
       * If we didn't clear it and we reached a commited transaction from an
       * aborted one, we could undo an already commited change.
       */
      this.transactions[transactionId].overwrites = []

      return 'committed'
    } catch (_e) {
      // Calling abort here to clear registers and reset old values
      this.abort(transactionId)

      return 'aborted'
    }
  }

  async schedule(
    transactionId: TransactionId,
    register: TransactionRegisterKey,
    operation: TransactionOperation,
    // TODO: remove the "value" argument
    value: null | DBRegisterValue
  ): Promise<{ action: Scheduled.Skip | Scheduled.Run } | ScheduleAbort> {
    if (this.transactions[transactionId] === undefined) {
      throw new Error('transaction not registered')
    }
    // TODO if transaction has been aborted

    const transactionTimestamp = this.transactions[transactionId].timestamp
    const registerReadTimestamp =
      this.readTimestamps[register] ?? UNUSED_PAST_TIMESTAMP
    const registerWriteTimestamp =
      this.writeTimestamps[register] ?? UNUSED_PAST_TIMESTAMP

    if (operation === TransactionOperation.Read) {
      if (registerWriteTimestamp > transactionTimestamp) {
        return {
          action: Scheduled.Abort,
          reason: AbortReason.ReadUncommited,
          conflictsWith: this.timestampToTransactions[registerWriteTimestamp],
        }
      }

      if (registerWriteTimestamp !== UNUSED_PAST_TIMESTAMP) {
        this.transactions[transactionId].dependsOn.push(
          this.timestampToTransactions[registerWriteTimestamp]
        )
      }

      this.readTimestamps[register] = Math.max(
        registerReadTimestamp,
        transactionTimestamp
      )
    } else {
      if (registerReadTimestamp > transactionTimestamp) {
        return {
          action: Scheduled.Abort,
          reason: AbortReason.RepeatableRead,
          conflictsWith: this.timestampToTransactions[registerReadTimestamp],
        }
      } else if (registerWriteTimestamp > transactionTimestamp) {
        return { action: Scheduled.Skip }
      }

      this.transactions[transactionId].overwrites.push([
        register,
        this.database[register],
        registerWriteTimestamp,
      ])
      this.writeTimestamps[register] = transactionTimestamp
    }

    return { action: Scheduled.Run }
  }

  private clearRegisters(transactionId: TransactionId): void {
    const transactionTimestamp = this.transactions[transactionId].timestamp
    const readRegisters = Object.keys(this.readTimestamps)
    const writeRegisters = Object.keys(this.writeTimestamps)

    readRegisters.forEach((register) => {
      if (this.readTimestamps[Number(register)] !== transactionTimestamp) {
        return
      }

      delete this.readTimestamps[Number(register)]
    })
    writeRegisters.forEach((register) => {
      if (this.writeTimestamps[Number(register)] !== transactionTimestamp) {
        return
      }

      delete this.writeTimestamps[Number(register)]
    })
  }
}
