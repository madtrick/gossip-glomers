import {
  log,
  MaelstromNodeId,
  Message,
  MessageBodyBroadcast,
  MessageBodyDeliver,
  MessageBodyDeliverOk,
  MessageBodyEcho,
  MessageBodyGenerate,
  MessageBodyInit,
  MessageBodyRead,
  MessageBodyTopology,
  MessageType,
  send,
  State,
} from './lib'

function handleInit(message: Message<MessageBodyInit>): State {
  const node = { id: message.body.node_id }
  send({
    src: node.id,
    dest: message.src,
    body: {
      type: MessageType.InitOk,
      in_reply_to: message.body.msg_id,
    },
  })
  return {
    node,
    nextMessageId: 0,
    outstandingMessages: [],
    broadcast: { messages: [], neighbours: [] },
  }
}

function handleEcho(state: State, message: Message<MessageBodyEcho>): State {
  send({
    src: state.node.id,
    dest: message.src,
    body: {
      type: MessageType.EchoOk,
      in_reply_to: message.body.msg_id,
      msg_id: message.body.msg_id,
      echo: message.body.echo,
    },
  })

  return state
}

function handleGenerate(
  state: State,
  message: Message<MessageBodyGenerate>
): State {
  send({
    dest: message.src,
    src: state.node.id,
    body: {
      type: MessageType.GenerateOk,
      in_reply_to: message.body.msg_id,
      id: Math.random(),
    },
  })

  return state
}

function deliver(state: State, dest: MaelstromNodeId, value: number): void {
  const msgId = state.nextMessageId
  state.nextMessageId += 1

  const message: Message<MessageBodyDeliver> = {
    src: state.node.id,
    dest,
    body: {
      type: MessageType.Deliver,
      message: value,
      msg_id: msgId,
    },
  }

  const retriedDelivery = () => {
    state.outstandingMessages[message.body.msg_id] = {
      timeout: setTimeout(() => {
        retriedDelivery()
        // TODO: use exponential backoff here
      }, 1000),
    }

    send(message)
  }

  retriedDelivery()
}

function handleBroadcast(
  state: State,
  message: Message<MessageBodyBroadcast>
): State {
  const { message: messageNumber } = message.body

  state.broadcast.messages.push(messageNumber)
  state.broadcast.neighbours.forEach((neighbourId) => {
    deliver(state, neighbourId, messageNumber)
  })

  send({
    src: state.node.id,
    dest: message.src,
    body: {
      type: MessageType.BroadcastOk,
      in_reply_to: message.body.msg_id,
    },
  })

  return state
}

function handleTopology(
  state: State,
  message: Message<MessageBodyTopology>
): State {
  const topology = message.body.topology
  const neighbours = topology[state.node.id]

  state.broadcast.neighbours = neighbours

  send({
    src: state.node.id,
    dest: message.src,
    body: {
      type: MessageType.TopologyOk,
      in_reply_to: message.body.msg_id,
    },
  })

  return state
}

function handleRead(state: State, message: Message<MessageBodyRead>): State {
  const messages = state.broadcast.messages

  send({
    src: state.node.id,
    dest: message.src,
    body: {
      messages,
      type: MessageType.ReadOk,
      in_reply_to: (message as Message<MessageBodyRead>).body.msg_id,
    },
  })

  return state
}

function handleDeliver(
  state: State,
  message: Message<MessageBodyDeliver>
): State {
  const { message: messageNumber } = message.body

  // TODO: handle re-delivery of the same message
  state.broadcast.messages.push(messageNumber)
  state.broadcast.neighbours.forEach((neighbourId) => {
    if (neighbourId === message.src) {
      return
    }

    deliver(state, neighbourId, messageNumber)
  })

  send({
    src: state.node.id,
    dest: message.src,
    body: {
      type: MessageType.DeliverOk,
      in_reply_to: message.body.msg_id,
    },
  })

  return state
}

function handleDeliverOk(
  state: State,
  message: Message<MessageBodyDeliverOk>
): State {
  clearTimeout(state.outstandingMessages[message.body.in_reply_to].timeout)
  delete state.outstandingMessages[message.body.in_reply_to]

  return state
}

function assertState(data: unknown): asserts data is State {
  if (data === undefined || data === null || typeof data !== 'object') {
    throw new Error('Invalid state')
  }

  if (!('node' in data)) {
    throw new Error('Invalid state')
  }
}

function handle(
  message: Message<
    | MessageBodyInit
    | MessageBodyEcho
    | MessageBodyGenerate
    | MessageBodyBroadcast
    | MessageBodyRead
    | MessageBodyTopology
    | MessageBodyDeliver
    | MessageBodyDeliverOk
  >,
  state?: State
): State | undefined {
  log(['recv', message])
  switch (message.body.type) {
    case MessageType.Init:
      log('Handle init')
      return handleInit(message as Message<MessageBodyInit>)
    case MessageType.Echo:
      log('Handle echo')
      assertState(state)

      return handleEcho(state, message as Message<MessageBodyEcho>)

    case MessageType.Generate:
      log('Handle generate')
      assertState(state)

      return handleGenerate(state, message as Message<MessageBodyGenerate>)

    case MessageType.Topology:
      log('Handle topology')
      assertState(state)

      return handleTopology(state, message as Message<MessageBodyTopology>)

    case MessageType.Broadcast:
      log('Handle broadcast')
      assertState(state)

      return handleBroadcast(state, message as Message<MessageBodyBroadcast>)

    case MessageType.Read:
      log('Handle read')
      assertState(state)

      return handleRead(state, message as Message<MessageBodyRead>)

    case MessageType.Deliver:
      log('Handle deliver')
      assertState(state)

      return handleDeliver(state, message as Message<MessageBodyDeliver>)
    case MessageType.DeliverOk:
      log('Handle deliver ok')
      assertState(state)

      return handleDeliverOk(state, message as Message<MessageBodyDeliverOk>)

    default:
      log('Unknown message')
      return state
  }
}

let state: State | undefined

// stdin is paused by default
process.stdin.resume()
process.stdin.on('data', (data) => {
  if (data) {
    log(['recv(string)', data.toString()])

    const messages = data.toString().trim().split('\n')
    state = messages.reduce((state, message) => {
      if (message.length > 0) {
        return handle(JSON.parse(message), state)
      } else {
        return state
      }
    }, state)
  }
})
