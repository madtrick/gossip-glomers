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
  // TODO: make the topology configurable
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
      // TODO remove the ts-ignore
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-ignore
      if (this.pendingRPCs[data.body.in_reply_to]) {
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore
        const callback = this.pendingRPCs[data.body.in_reply_to].callback
        callback && callback(this, this.state, data)

        clearTimeout(
          this.pendingRPCs[(data.body as unknown as ReplyMessage).in_reply_to]
            .timeout
        )
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore
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

  // TODO: can't I combine the rpc and rpcSync in one method?
  // to make it sync the caller can await the promise
  rpc(
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

    log(`[rpc] ${JSON.stringify(message)}`)

    // if (callback) {
    //   this.pendingRPCs[msgId] = callback
    // }
    this.pendingRPCs[msgId] = {
      callback,
      timeout: setTimeout(() => {
        log('[send] timeout expired')
        if (this.pendingRPCs[msgId] === undefined) {
          // TODO: is this case even posible. If the reply has been received
          // and this value set to `undefined` then the timout must have been
          // cleared too
          log('[send] reply received - not retrying')
          return
        }

        log('[send] reply not received - retrying')
        this.rpc(dest, data, callback)
      }, 1000),
    }
    this.nextMessageId += 1
    this.outputChannel.push(message)
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  rpcSync(dest: MaelstromNodeId, data: Record<string, unknown>): Promise<any> {
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

        this.pendingRPCs[msgId] = {
          callback,
          timeout: setTimeout(() => {
            log('[rpcSync] timeout expired')
            if (this.pendingRPCs[msgId] === undefined) {
              log('[rpcSync] reply received - not retrying')
              return
            }

            log('[rpcSync] reply not received - retrying')
            send()
          }, 1000),
        }

        log(`[rpcSync] ${JSON.stringify(message)}`)
        this.nextMessageId += 1
        this.outputChannel.push(message)
      }

      send()
    })
  }

  send(dest: MaelstromNodeId, data: Record<string, unknown>) {
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

    this.nextMessageId += 1
    this.outputChannel.push(message)
  }
}
