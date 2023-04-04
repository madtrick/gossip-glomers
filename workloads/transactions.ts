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
} from '../lib'
import { ANode } from '../node'

type DBKey = number
type DBValue = number

type State = {
  db: Record<DBKey, DBValue>
}

const input = new ConsoleInputChannel()
const output = new ConsoleOutputchannel()
const node = new ANode<State>({ db: {} }, input, output)

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

  const result: Array<TransactionAction> = []
  const applyTxn = (txn: Array<TransactionAction>) => {
    const action = txn.shift()

    if (action === undefined) {
      return
    }

    const [op, key, value] = action

    log(`[txn] msg_id ${msg_id} src ${src} apply ${action}`)
    if (op === TransactionOperation.Read) {
      result.push([op, key, state.db[key] ?? null])
    } else {
      state.db[key] = value

      result.push(action)
    }

    setImmediate(applyTxn, txn)
    // setTimeout(applyTxn, 5, txn)
  }

  applyTxn(txn)

  node.send(src, {
    type: MessageType.TxnOK,
    txn: result,
    in_reply_to: msg_id,
  })
})
