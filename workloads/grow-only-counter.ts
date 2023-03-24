import {
  ConsoleInputChannel,
  ConsoleOutputchannel,
  log,
  MaelstromNode,
  Message,
  MessageBodyAdd,
  MessageBodyInit,
  MessageBodyKVReadOk,
  MessageBodyRead,
  MessageType,
} from '../lib'
import { ANode } from '../node'

interface State {
  // The counter is used for debugging purposes
  counter: number
}

const KVID = 'seq-kv'
const KVKEY = 'value'

const input = new ConsoleInputChannel()
const output = new ConsoleOutputchannel()
const node = new ANode<State>({ counter: 0 }, input, output)

async function updateCounter<State>(
  node: MaelstromNode<State>,
  delta: number
): Promise<void> {
  /**
   * IMPORTANT: await here doesn't prevent the node from receiving and
   * handling other messages
   */
  const reply = await node.rpcSync(KVID, {
    type: MessageType.KVRead,
    key: KVKEY,
  })
  const readMessage = reply as Message<MessageBodyKVReadOk>

  log(
    `[update value] ${readMessage.body.value} / ${
      readMessage.body.value + delta
    }`
  )

  let casReply = await node.rpcSync(KVID, {
    type: MessageType.KVCas,
    key: KVKEY,
    from: readMessage.body.value,
    to: readMessage.body.value + delta,
  })

  while (casReply.body.type === MessageType.Error) {
    const reply = await node.rpcSync(KVID, {
      type: MessageType.KVRead,
      key: KVKEY,
    })
    const readMessage = reply as Message<MessageBodyKVReadOk>

    casReply = await node.rpcSync(KVID, {
      type: MessageType.KVCas,
      key: KVKEY,
      from: readMessage.body.value,
      to: readMessage.body.value + delta,
    })
  }
  // } while (casReply.body.type === MessageType.Error)
}

node.on(MessageType.Init, async (node, state, message) => {
  const initMessage = message as Message<MessageBodyInit>
  const {
    body: { node_id: nodeId },
  } = initMessage

  node.id = nodeId

  node.send(KVID, {
    type: MessageType.KVCas,
    key: KVKEY,
    from: 0,
    to: 0,
    create_if_not_exists: true,
  })

  node.send(message.src, {
    type: MessageType.InitOk,
    in_reply_to: initMessage.body.msg_id,
  })
})

node.on(MessageType.Read, async (node, _state, message) => {
  const readMessage = message as Message<MessageBodyRead>

  let casReply: Message
  let replyReadMessage: Message<MessageBodyKVReadOk>

  do {
    const replyRead = await node.rpcSync(KVID, {
      type: MessageType.KVRead,
      key: KVKEY,
    })
    replyReadMessage = replyRead as Message<MessageBodyKVReadOk>

    casReply = await node.rpcSync(KVID, {
      type: MessageType.KVCas,
      key: KVKEY,
      from: replyReadMessage.body.value,
      to: replyReadMessage.body.value,
    })
  } while (casReply.body.type === MessageType.Error)

  node.send(readMessage.src, {
    type: MessageType.ReadOk,
    value: replyReadMessage.body.value,
    in_reply_to: readMessage.body.msg_id,
  })
})

node.on(MessageType.Add, async (node, state, message) => {
  const addMessage = message as Message<MessageBodyAdd>
  const {
    body: { delta, msg_id: msgId },
  } = addMessage

  state.counter += delta
  log(`[counter] ${state.counter}`)
  await updateCounter(node, delta)

  node.send(addMessage.src, {
    type: MessageType.AddOk,
    in_reply_to: msgId,
  })
})
