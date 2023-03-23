import {
  ConsoleInputChannel,
  ConsoleOutputchannel,
  MaelstromNode,
  Message,
  MessageBodyBroadcast,
  MessageBodyDeliver,
  MessageBodyRead,
  MessageId,
  MessageType,
} from '../lib'
import { ANode } from '../node'

interface State {
  messages: Array<number>
  outstandingMessages: Array<MessageId>
}

function broadcast<State>(node: MaelstromNode<State>, value: number): void {
  node.neighbours.forEach((neighbour) => {
    const retriedDelivery = () => {
      const timeout = setTimeout(retriedDelivery, 1000)

      node.send(
        neighbour,
        {
          type: MessageType.Deliver,
          message: value,
          broadcast_to: node.id,
        },
        (_node, _state, reply) => {
          if (reply.body.type !== MessageType.DeliverOk) {
            throw new Error('Unexpected message type')
          }

          clearTimeout(timeout)
        }
      )
    }

    retriedDelivery()
  })
}

const input = new ConsoleInputChannel()
const output = new ConsoleOutputchannel()
const node = new ANode<State>(
  { messages: [], outstandingMessages: [] },
  input,
  output
)

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
  broadcast(node, value)

  node.send(src, {
    type: MessageType.BroadcastOk,
    in_reply_to: broadcastMessageId,
  })
})

node.on(MessageType.Deliver, (node, state, message) => {
  const broadcastMessage = message as Message<MessageBodyDeliver>
  const { message: deliveredMessage, broadcast_to } = broadcastMessage.body

  if (broadcast_to === node.id) {
    return
  }

  // TODO: handle re-delivery of the same message
  state.messages.push(deliveredMessage)
  // node.neighbours.forEach((neighbour) => {
  //   node.send(neighbour, {
  //     type: MessageType.Deliver,
  //     message: deliveredMessage,
  //     broadcast_to: node.id,
  //   })
  // })
  broadcast(node, deliveredMessage)

  node.send(message.src, {
    type: MessageType.DeliverOk,
    in_reply_to: (message as Message<MessageBodyDeliver>).body.msg_id,
  })
})
