import {
  ConsoleInputChannel,
  ConsoleOutputchannel,
  log,
  Message,
  MessageBodyTxn,
  MessageType,
  TransactionAction,
  TransactionOperation,
  TransactionRegisterKey,
} from '../lib'
import { hasWriteWrite } from '../lib/dependency-graph'
import { Scheduled, TimestampedScheduler } from '../lib/timestamped-scheduler'
import { ANode } from '../node'

type DBRegisterKey = number
export type DBRegisterValue = number
export type Database = Record<DBRegisterKey, DBRegisterValue>
export type TransactionId = number
export type TransactionOperationIndex = number

type State = {
  db: Record<DBRegisterKey, DBRegisterValue>
  transactionCounter: number
  transactionOperationCounter: TransactionOperationIndex
  scheduler: TimestampedScheduler
  // TODO: remove the inflightOperations
  inflightOperations: Array<
    [TransactionId, TransactionOperationIndex, TransactionRegisterKey]
  >
}

const input = new ConsoleInputChannel()
const output = new ConsoleOutputchannel()
const DATABASE = {}
const node = new ANode<State>(
  {
    db: DATABASE,
    transactionOperationCounter: 0,
    transactionCounter: 0,
    inflightOperations: [],
    scheduler: new TimestampedScheduler(DATABASE),
  },
  input,
  output
)

node.on(MessageType.Txn, async (node, state, message) => {
  const {
    src,
    body: { txn, msg_id },
  } = message as Message<MessageBodyTxn>

  node.neighbours.forEach((neighbour) => {
    node.send(neighbour, {
      type: MessageType.TxReplicate,
      txn,
    })
  })
  const result: Array<TransactionAction> = []
  const applyTxn = async (
    transactionId: TransactionId,
    txnActions: Array<TransactionAction>
  ) => {
    const action = txnActions.shift()

    if (action === undefined) {
      // Remove inflight operations belonging to the transactio that just finished
      state.inflightOperations = state.inflightOperations.filter(
        (inflightOperation) => {
          const txid = inflightOperation[0]

          return txid !== transactionId
        }
      )

      log(`[txn] transaction ${transactionId} commit ${txn}`)
      node.send(src, {
        type: MessageType.TxnOK,
        txn: result,
        in_reply_to: msg_id,
      })

      return
    }

    const [op, key, value] = action

    const operationId = state.transactionOperationCounter
    state.transactionOperationCounter += 1

    const { action: schedulerAction } = await state.scheduler.schedule(
      transactionId,
      key,
      op,
      value
    )
    log(
      `[txn] scheduler action ${schedulerAction} (tx ${transactionId} ${op} ${key})`
    )

    if (schedulerAction === Scheduled.Skip) {
      setImmediate(applyTxn, transactionId, txnActions)
      return
    }

    if (schedulerAction === Scheduled.Abort) {
      await state.scheduler.abort(transactionId)
      // Schedule it again (another attempt)
      // With setImmediate we can run into a loop with the transaction
      // that caused the abort in the first place
      // setImmediate(execute, [...txn])
      setTimeout(execute, Math.random() * 10, [...txn])
      return
    }

    log(
      `[txn] msg_id ${msg_id} src ${src} apply ${action} (tx ${transactionId} op ${operationId})`
    )

    if (op === TransactionOperation.Read) {
      result.push([op, key, state.db[key] ?? null])
    } else {
      /**
       * Note that we only keep "write" operations in the in-flight array. We
       * are only looking for write-write cycles
       */
      state.inflightOperations.push([transactionId, operationId, key])
      state.db[key] = value

      if (hasWriteWrite(state.inflightOperations)) {
        log(
          `[txn] write-write cycle ${JSON.stringify(state.inflightOperations)}`
        )
      }

      result.push(action)
    }

    // state.transactionOperationCounter += 1
    setImmediate(applyTxn, transactionId, txnActions)
  }

  const execute = async (actions: Array<TransactionAction>) => {
    const transactionId = state.transactionCounter
    state.transactionCounter += 1

    await state.scheduler.register(transactionId)

    applyTxn(transactionId, actions)
  }

  execute([...txn])
})

node.on(MessageType.TxReplicate, (_node, state, message) => {
  const {
    src,
    body: { txn, msg_id },
  } = message as Message<MessageBodyTxn>
  const transactionId = state.transactionCounter
  state.transactionCounter += 1

  const applyTxn = (txn: Array<TransactionAction>) => {
    const action = txn.shift()

    if (action === undefined) {
      state.inflightOperations = state.inflightOperations.filter(
        (inflightOperation) => {
          const txid = inflightOperation[0]

          return txid !== transactionId
        }
      )
      return
    }

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const [_op, key, value] = action

    const operationId = state.transactionOperationCounter
    state.transactionOperationCounter += 1

    log(
      `[txn] msg_id ${msg_id} src ${src} apply ${action} (tx ${transactionId} op ${operationId})`
    )
    /**
     * Note that we only keep "write" operations in the in-flight array. We
     * are only looking for write-write cycles
     */
    state.inflightOperations.push([transactionId, operationId, key])
    state.db[key] = value

    if (hasWriteWrite(state.inflightOperations)) {
      log(`[txn] write-write cycle ${JSON.stringify(state.inflightOperations)}`)
    }

    state.transactionOperationCounter += 1
    setImmediate(applyTxn, txn)
  }

  applyTxn([...txn])
})
