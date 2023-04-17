import {
  ConsoleInputChannel,
  ConsoleOutputchannel,
  MaelstromNode,
  MaelstromNodeId,
  Message,
  MessageBodyBroadcast,
  MessageBodyDeliver,
  MessageBodyRead,
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
  const { message: deliveredMessage, broadcast_to } = broadcastMessage.body

  // TODO: handle re-delivery of the same message
  state.messages.push(deliveredMessage)
  broadcast(node, deliveredMessage, broadcast_to)

  node.send(message.src, {
    type: MessageType.DeliverOk,
    in_reply_to: (message as Message<MessageBodyDeliver>).body.msg_id,
  })
})
