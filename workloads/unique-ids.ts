import {
  ConsoleInputChannel,
  ConsoleOutputchannel,
  MessageBodyGenerate,
  MessageType,
} from '../lib'
import { ANode } from '../node'

type State = { counter: number }

const input = new ConsoleInputChannel()
const output = new ConsoleOutputchannel()
const node = new ANode<State>({ counter: 0 }, input, output)

node.on(MessageType.Generate, (node, state, message) => {
  node.send(message.src, {
    type: MessageType.GenerateOk,
    in_reply_to: (message.body as MessageBodyGenerate).msg_id,
    id: `${node.id}-${state.counter}`,
  })

  state.counter += 1
})
