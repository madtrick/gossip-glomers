import {
  ConsoleInputChannel,
  ConsoleOutputchannel,
  Message,
  MessageBodyGenerate,
  MessageType,
} from '../lib'
import { ANode } from '../node'

const input = new ConsoleInputChannel()
const output = new ConsoleOutputchannel()
const node = new ANode({}, input, output)

node.on(MessageType.Generate, (node, _state, message) => {
  node.send(message.src, {
    type: MessageType.GenerateOk,
    in_reply_to: (message as Message<MessageBodyGenerate>).body.msg_id,
    id: Math.random(),
  })
})
