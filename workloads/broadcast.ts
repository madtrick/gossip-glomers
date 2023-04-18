import {
  ConsoleInputChannel,
  ConsoleOutputchannel,
  log,
  MaelstromNode,
  MaelstromNodeId,
  Message,
  MessageBodyBroadcast,
  MessageBodyDeliver,
  MessageBodyRead,
  MessageBodyTopology,
  MessageType,
} from '../lib'
import { ANode } from '../node'

interface State {
  messages: Array<number>
}

function broadcast<State>(
  node: MaelstromNode<State>,
  value: number,
  broadcastTo: MaelstromNodeId
): void {
  node.neighbours.forEach((neighbour) => {
    if (broadcastTo === neighbour) {
      // Do not broadcast to the node that started the broadcast chain
      return
    }

    node.rpc(neighbour, {
      type: MessageType.Deliver,
      message: value,
      broadcast_to: broadcastTo,
    })
  })
}

const input = new ConsoleInputChannel()
const output = new ConsoleOutputchannel()
const node = new ANode<State>({ messages: [] }, input, output)

node.on(MessageType.Topology, (node, _state, message) => {
  const { body } = message as Message<MessageBodyTopology>
  const nodeIds = new Set()

  for (const key in body.topology) {
    nodeIds.add(key)
    body.topology[key].forEach(nodeIds.add.bind(nodeIds))
  }

  log(`[topology] node ids ${Array.from(nodeIds.values())}`)

  const idNumber = Number(node.id.substring(1))
  const neighbours = []

  if (idNumber % 6 === 0) {
    log(`[topology] node id ${node.id} is head`)

    for (let i = 1; i < 6 && idNumber + i < nodeIds.size; i++) {
      neighbours.push(`n${idNumber + i}`)
    }

    if (idNumber + 6 < nodeIds.size) {
      neighbours.push(`n${idNumber + 6}`)
    }

    if (idNumber - 6 >= 0) {
      neighbours.push(`n${idNumber - 6}`)
    }
  } else {
    const offset = idNumber % 6
    neighbours.push(`n${idNumber - offset}`)
  }

  log(`[topology] node id ${node.id} neighbours ${neighbours}`)

  node.neighbours = neighbours

  node.send(message.src, {
    type: MessageType.TopologyOk,
    in_reply_to: body.msg_id,
  })
})
node.on(MessageType.Read, (node, state, message) => {
  node.send(message.src, {
    type: MessageType.ReadOk,
    in_reply_to: (message as Message<MessageBodyRead>).body.msg_id,
    messages: state.messages,
  })
})

node.on(MessageType.Broadcast, (node, state, message) => {
  const broadcastMessage = message as Message<MessageBodyBroadcast>
  const { src } = broadcastMessage
  const { message: value, msg_id: broadcastMessageId } = broadcastMessage.body

  state.messages.push(value)
  broadcast(node, value, node.id)

  node.send(src, {
    type: MessageType.BroadcastOk,
    in_reply_to: broadcastMessageId,
  })
})

node.on(MessageType.Deliver, (node, state, message) => {
  const broadcastMessage = message as Message<MessageBodyDeliver>
  const { src } = broadcastMessage
  const { message: deliveredMessage } = broadcastMessage.body

  // TODO: handle re-delivery of the same message
  state.messages.push(deliveredMessage)
  broadcast(node, deliveredMessage, src)

  node.send(message.src, {
    type: MessageType.DeliverOk,
    in_reply_to: (message as Message<MessageBodyDeliver>).body.msg_id,
  })
})
