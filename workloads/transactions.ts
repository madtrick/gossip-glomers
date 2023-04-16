import {
  ConsoleInputChannel,
  ConsoleOutputchannel,
  log,
  Message,
  MessageBodyTxn,
  MessageType,
  TransactionAction,
  TransactionOperation,
} from '../lib'
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
}

const input = new ConsoleInputChannel()
const output = new ConsoleOutputchannel()
const DATABASE = {}
const node = new ANode<State>(
  {
    db: DATABASE,
    transactionOperationCounter: 0,
    transactionCounter: 0,
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

  let result: Array<TransactionAction> = []
  const applyTxn = async (
    transactionId: TransactionId,
    txnActions: Array<TransactionAction>
  ) => {
    const action = txnActions.shift()

    if (action === undefined) {
      const scheduled = await state.scheduler.commit(transactionId)

      if (scheduled === 'aborted') {
        log(`[txn] transaction ${transactionId} aborted on commit ${txn}`)
        result = []
        setTimeout(execute, Math.random() * 10, [...txn])
        return
      }

      log(`[txn] transaction ${transactionId} commit ${txn}`)
      node.send(src, {
        type: MessageType.TxnOK,
        txn: result,
        in_reply_to: msg_id,
      })
      node.neighbours.forEach((neighbour) => {
        node.send(neighbour, {
          type: MessageType.TxReplicate,
          txn,
        })
      })

      return
    }

    const [op, key, value] = action

    const operationId = state.transactionOperationCounter
    state.transactionOperationCounter += 1

    const scheduled = await state.scheduler.schedule(
      transactionId,
      key,
      op,
      value
    )
    log(
      `[txn] scheduler action ${scheduled.action} (tx ${transactionId} ${op} ${key})`
    )

    if (scheduled.action === Scheduled.Skip) {
      // We have to include this in the result as from the perspective of the client
      // the operation executed.
      //
      // What I'm not sure about is wether I should return the value set in the action
      // or the current database value (i.e. te value of the latest write)
      result.push(action)
      setImmediate(applyTxn, transactionId, txnActions)
      return
    }

    if (scheduled.action === Scheduled.Abort) {
      log(
        `[txn] abort ${transactionId} reason "${scheduled.reason} ${scheduled.conflictsWith}"`
      )
      await state.scheduler.abort(transactionId)
      // Schedule it again (another attempt)
      // With setImmediate we can run into a loop with the transaction
      // that caused the abort in the first place
      // setImmediate(execute, [...txn])
      result = []
      setTimeout(execute, Math.random() * 10, [...txn])
      return
    }

    log(
      `[txn] msg_id ${msg_id} src ${src} apply ${action} (tx ${transactionId} op ${operationId})`
    )

    if (op === TransactionOperation.Read) {
      result.push([op, key, state.db[key] ?? null])
    } else {
      state.db[key] = value

      result.push(action)
    }

    setImmediate(applyTxn, transactionId, txnActions)
  }

  const ids: Array<TransactionId> = []
  const execute = async (actions: Array<TransactionAction>) => {
    const transactionId = state.transactionCounter
    state.transactionCounter += 1

    ids.push(transactionId)

    log(`[txn] execute ${ids}`)

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

  txn.forEach((action) => {
    const [op, key, value] = action

    if (op === TransactionOperation.Write) {
      log(`[txn replicate] msg_id ${msg_id} src ${src} apply ${action}`)
      state.db[key] = value
    }
  })
})
