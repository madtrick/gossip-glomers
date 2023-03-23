import {
  InputChannel,
  log,
  MaelstromNode,
  MaelstromNodeId,
  Message,
  MessageBodyInit,
  MessageBodyTopology,
  MessageHandler,
  MessageId,
  MessageType,
  OutputChannel,
  TypableMessage,
} from './lib'

export function handleInitMessage<State>(
  node: MaelstromNode<State>,
  // TODO: drop the state from this function
  state: State,
  message: Message<MessageBodyInit>
): State {
  const {
    body: { node_ids: nodeIds, node_id: nodeId },
  } = message

  node.id = nodeId
  node.send(message.src, {
    type: MessageType.InitOk,
    in_reply_to: message.body.msg_id,
  })

  /**
   * Ignore the topology set by maelstrom and instead set a ring
   *
   * Ignoring the topology is mentioned in the exercises description. I chose
   * to create a ring for a more straightforward setup where each node only
   * "broadcasts" to the next node in line. This is definitely less fault
   * tolerant than having more redundancy but works for the exercises.
   */
  if (nodeIds.length === 1) {
    node.neighbours = []
  } else {
    const indexOfNode = nodeIds.indexOf(nodeId)
    const neighbour =
      indexOfNode === nodeIds.length - 1 ? nodeIds[0] : nodeIds[indexOfNode + 1]
    node.neighbours = [neighbour]
  }

  return state
}

// function noopMessageHandler<State>(
//   // eslint-disable-next-line @typescript-eslint/no-unused-vars
//   _node: MaelstromNode<State>,
//   // eslint-disable-next-line @typescript-eslint/no-unused-vars
//   _state: State,
//   // eslint-disable-next-line @typescript-eslint/no-unused-vars
//   _message: Message
// ): void {
//   return
// }

function assertMessage(
  data: unknown
): asserts data is Message<TypableMessage<MessageType>> {
  if (data === null || typeof data !== 'object') {
    throw new Error('Message must be an object')
  }
}

function assertMessageType<T extends MessageType>(
  message: Message,
  type: T
): asserts message is Message<TypableMessage<T>> {
  if (message.body.type !== type) {
    throw new Error('Unexpected message type')
  }
}

export class ANode<State> implements MaelstromNode<State> {
  private inputChannel: InputChannel
  private outputChannel: OutputChannel
  private messageHandlers: Partial<Record<MessageType, MessageHandler<State>>>
  private nextMessageId: number
  private pendingRPCs: Record<MessageId, MessageHandler<State>>
  neighbours: Array<MaelstromNodeId>
  state: State
  id: MaelstromNodeId

  constructor(state: State, input: InputChannel, output: OutputChannel) {
    this.state = state
    this.id = '-1'
    this.nextMessageId = 0
    this.neighbours = []
    this.pendingRPCs = {}
    this.inputChannel = input
    this.inputChannel.attach((data: unknown) => {
      assertMessage(data)

      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-ignore
      if (this.pendingRPCs[data.body.in_reply_to]) {
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore
        this.pendingRPCs[data.body.in_reply_to](this, this.state, data)
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore
        delete this.pendingRPCs[data.body.in_reply_to]
      } else {
        const messageType: MessageType = data.body.type
        const handler = this.messageHandlers[messageType]

        log(`[recv] ${JSON.stringify(data)}`)

        if (handler) {
          log(`Handling "${messageType}"`)
          handler(this, this.state, data)
        } else {
          log(`Unhandled message ${messageType}`)
        }
      }
    })
    this.outputChannel = output
    this.messageHandlers = {
      [MessageType.Init]: (
        node: MaelstromNode<State>,
        state: State,
        data: Message<TypableMessage>
      ) => {
        assertMessageType(data, MessageType.Init)

        this.state = handleInitMessage(
          node,
          state,
          data as Message<MessageBodyInit>
        )
      },
      [MessageType.Topology]: (
        _node: MaelstromNode<State>,
        _state: State,
        message: Message
      ) => {
        /**
         * See handleInitMessage for an explanation as to why we don't do
         * anything with the topology information
         */
        this.send(message.src, {
          type: MessageType.TopologyOk,
          in_reply_to: (message as Message<MessageBodyTopology>).body.msg_id,
        })
      },
    }
  }

  on(type: MessageType, callback: MessageHandler<State>): void {
    this.messageHandlers[type] = callback
  }

  send(
    dest: MaelstromNodeId,
    data: Record<string, unknown>,
    callback?: MessageHandler<State>
  ): void {
    const msgId = this.nextMessageId
    const message = {
      dest,
      src: this.id,
      body: {
        msg_id: msgId,
        ...data,
      },
    }

    log(`[send] ${JSON.stringify(message)}`)

    if (callback) {
      this.pendingRPCs[msgId] = callback
    }
    this.nextMessageId += 1
    this.outputChannel.push(message)
  }

  // rpc(
  //   dest: MaelstromNodeId,
  //   data: Record<string, unknown>,
  //   callback: RPCCallback
  // ) {
  //   const msgId = this.nextMessageId
  //   const message = {
  //     dest,
  //     src: this.id,
  //     body: {
  //       msg_id: msgId,
  //       ...data,
  //     },
  //   }

  //   log(`[rpc] ${JSON.stringify(message)}`)

  //   this.pendingRPCs[msgId] = callback

  //   this.nextMessageId += 1
  //   this.outputChannel.push(message)
  // }
}
