import { TransactionOperation, TransactionRegisterKey } from '../lib'
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

const UNUSED_PAST_TIMESTAMP = 0

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
      registers: Set<TransactionRegisterKey>
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
      registers: new Set(),
      overwrites: [],
    }
    this.nextTimestamp += 1

    return Promise.resolve()
  }

  async abort(transactionId: TransactionId): Promise<void> {
    // Revert old values
    this.transactions[transactionId].overwrites.forEach(
      ([register, value, timestamp]) => {
        if (
          this.writeTimestamps[register] ===
          this.transactions[transactionId].timestamp
        ) {
          this.database[register] = value
          this.writeTimestamps[register] = timestamp
        }
      }
    )
    this.transactions[transactionId].deferred.reject()
    delete this.transactions[transactionId]
  }

  async commit(transactionId: TransactionId): Promise<'committed' | 'aborted'> {
    const promises = this.transactions[transactionId].dependsOn.map((txid) => {
      if (this.transactions[txid] === undefined) {
        throw new Error('unexpected transaction not found')
      }

      return this.transactions[txid].deferred.promise
    })

    try {
      await Promise.all(promises)
      this.transactions[transactionId].deferred.resolve()
      delete this.transactions[transactionId]
      return 'committed'
    } catch (_e) {
      return 'aborted'
    }
  }

  async schedule(
    transactionId: TransactionId,
    register: TransactionRegisterKey,
    operation: TransactionOperation,
    value: null | DBRegisterValue
  ): Promise<{ action: Scheduled }> {
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
        return { action: Scheduled.Abort }
      }

      if (registerWriteTimestamp !== UNUSED_PAST_TIMESTAMP) {
        this.transactions[transactionId].dependsOn.push(
          this.timestampToTransactions[registerWriteTimestamp]
        )
      }

      this.transactions[transactionId].registers.add(register)
      this.readTimestamps[register] = Math.max(
        registerReadTimestamp,
        transactionTimestamp
      )
    } else {
      if (registerReadTimestamp > transactionTimestamp) {
        return { action: Scheduled.Abort }
      } else if (registerWriteTimestamp > transactionTimestamp) {
        return { action: Scheduled.Skip }
      }

      this.transactions[transactionId].registers.add(register)
      // TODO test this
      this.transactions[transactionId].overwrites.push([
        register,
        this.database[register],
        transactionTimestamp,
      ])
      this.writeTimestamps[register] = transactionTimestamp
    }

    return { action: Scheduled.Run }
  }
}
