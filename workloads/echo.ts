import {
  ConsoleInputChannel,
  ConsoleOutputchannel,
  Message,
  MessageBodyEcho,
  MessageType,
} from '../lib'
import { ANode } from '../node'

const input = new ConsoleInputChannel()
const output = new ConsoleOutputchannel()
const node = new ANode({}, input, output)

function assertMessageIsEcho(
  message: Message
): asserts message is Message<MessageBodyEcho> {
  if (message.body.type !== MessageType.Echo) {
    throw new Error('Unexpected message type')
  }
}

node.on(MessageType.Echo, (node, _state, message) => {
  assertMessageIsEcho(message)

  node.send(message.src, {
    type: MessageType.EchoOk,
    in_reply_to: message.body.msg_id,
    echo: message.body.echo,
  })
})
