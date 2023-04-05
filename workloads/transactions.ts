import {
  ConsoleInputChannel,
  ConsoleOutputchannel,
  log,
  Message,
  MessageBodyInit,
  MessageBodyTxn,
  MessageType,
  TransactionAction,
  TransactionOperation,
  TransactionRegisterKey,
} from '../lib'
import { hasWriteWrite } from '../lib/graph-dependency'
import { ANode } from '../node'

type DBRegisterKey = number
type DBRegisterValue = number
export type TransactionId = number
export type TransactionOperationIndex = number

type State = {
  db: Record<DBRegisterKey, DBRegisterValue>
  transactionCounter: number
  transactionOperationCounter: TransactionOperationIndex
  inflightOperations: Array<
    [TransactionId, TransactionOperationIndex, TransactionRegisterKey]
  >
}

const input = new ConsoleInputChannel()
const output = new ConsoleOutputchannel()
const node = new ANode<State>(
  {
    db: {},
    transactionOperationCounter: 0,
    transactionCounter: 0,
    inflightOperations: [],
  },
  input,
  output
)

node.on(MessageType.Init, async (node, _state, message) => {
  const initMessage = message as Message<MessageBodyInit>
  const {
    body: { node_id: nodeId },
  } = initMessage

  node.id = nodeId
  node.send(message.src, {
    type: MessageType.InitOk,
    in_reply_to: initMessage.body.msg_id,
  })
})

node.on(MessageType.Txn, (node, state, message) => {
  const {
    src,
    body: { txn, msg_id },
  } = message as Message<MessageBodyTxn>
  const transactionId = state.transactionCounter
  state.transactionCounter += 1

  const result: Array<TransactionAction> = []
  const applyTxn = (txn: Array<TransactionAction>) => {
    const action = txn.shift()

    if (action === undefined) {
      state.inflightOperations = state.inflightOperations.filter(
        (inflightOperation) => {
          const txid = inflightOperation[0]

          return txid !== transactionId
        }
      )
      // delete state.inflightTransactions[transactionId]
      return
    }

    const [op, key, value] = action

    const operationId = state.transactionOperationCounter
    state.transactionOperationCounter += 1

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

    state.transactionOperationCounter += 1
    setImmediate(applyTxn, txn)
  }

  applyTxn(txn)

  node.send(src, {
    type: MessageType.TxnOK,
    txn: result,
    in_reply_to: msg_id,
  })
})
