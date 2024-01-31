import {
  assertMessageType,
  InputChannel,
  log,
  MaelstromNode,
  MaelstromNodeId,
  Message,
  MessageBodyInit,
  MessageBodyTopology,
  MessageHandler,
  MessageId,
  messageIsReply,
  MessageType,
  OutputChannel,
  ReplyMessage,
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

  node.send(message.src, {
    type: MessageType.InitOk,
    in_reply_to: message.body.msg_id,
  })

  return state
}

function assertMessage(
  data: unknown
): asserts data is Message<TypableMessage<MessageType>> {
  if (data === null || typeof data !== 'object') {
    throw new Error('Message must be an object')
  }
}

export class ANode<State> implements MaelstromNode<State> {
  private inputChannel: InputChannel
  private outputChannel: OutputChannel
  private messageHandlers: Partial<Record<MessageType, MessageHandler<State>>>
  private nextMessageId: number
  private pendingRPCs: Record<
    MessageId,
    {
      callback?: MessageHandler<State>
      timeout: NodeJS.Timeout
    }
  >
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

      log(
        `[recv][from:${data.src}][type:${data.body.type}][msg_id:${
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          (data.body as any).msg_id
        }] ${JSON.stringify(data)}`
      )
      /**
       * When a message is received and that message is a reply
       * to a message sent from this node, we clear the timeout
       * so we don't retry the delivery again
       */
      if (messageIsReply(data) && this.pendingRPCs[data.body.in_reply_to]) {
        const callback = this.pendingRPCs[data.body.in_reply_to].callback
        callback && callback(this, this.state, data)

        clearTimeout(
          this.pendingRPCs[(data.body as unknown as ReplyMessage).in_reply_to]
            .timeout
        )

        delete this.pendingRPCs[data.body.in_reply_to]
      } else {
        const messageType: MessageType = data.body.type
        const handler = this.messageHandlers[messageType]

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

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  rpc(
    dest: MaelstromNodeId,
    data: Record<string, unknown>,
    options: { noresponse: boolean } = { noresponse: false }
  ): Promise<any> {
    return new Promise((resolve) => {
      const callback: MessageHandler<State> = (_node, _state, message) => {
        resolve(message)
      }

      const send = () => {
        const msgId = this.nextMessageId
        const message = {
          dest,
          src: this.id,
          body: {
            msg_id: msgId,
            ...data,
          },
        }

        if (!options.noresponse) {
          this.pendingRPCs[msgId] = {
            callback,
            timeout: setTimeout(() => {
              log('[rpc] timeout expired')
              if (this.pendingRPCs[msgId] === undefined) {
                log('[rpc] reply received - not retrying')
                return
              }

              log('[rpc] reply not received - retrying')
              send()
            }, 1000),
          }
        }

        log(`[rpc] ${JSON.stringify(message)}`)
        this.nextMessageId += 1
        this.outputChannel.push(message)
      }

      send()
    })
  }

  async send(
    dest: MaelstromNodeId,
    data: Record<string, unknown>
  ): Promise<void> {
    this.rpc(dest, data, { noresponse: true })
  }
}
