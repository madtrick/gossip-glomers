enum MessageType {
  Init = 'init',
  InitOk = 'init_ok',
  Echo = 'echo',
  EchoOk = 'echo_ok',
}

interface MessageBodyInit {
  type: MessageType.Init
  msg_id: string
  node_id: string
}

interface MessageBodyEcho {
  type: MessageType.Echo
  msg_id: number
  echo: string
}

interface Message<B> {
  src: string
  dest: string
  body: B
}

const log = (data: any) =>
  console.error(`[${Date.now()}]: ${JSON.stringify(data)}`)

function handleInit(message: Message<MessageBodyInit>): MaelstromNode {
  const node = { id: message.body.node_id }

  return node
}

function assertState(data: unknown): asserts data is State {
  if (data === undefined || data === null || typeof data !== 'object') {
    throw new Error('Invalid state')
  }

  if (!('node' in data)) {
    throw new Error('Invalid state')
  }
}

function send(data: any): void {
  log(['send', data])
  console.log(JSON.stringify(data))
}

function handle(
  message: Message<MessageBodyInit | MessageBodyEcho>,
  state?: State
): State | undefined {
  log(['recv', message])
  switch (message.body.type) {
    case MessageType.Init:
      log('Handle init')
      const node = handleInit(message as Message<MessageBodyInit>)
      send({
        src: node.id,
        dest: message.src,
        body: {
          type: MessageType.InitOk,
          in_reply_to: message.body.msg_id,
        },
      })
      return { node }
    case MessageType.Echo:
      log('Handle echo')
      assertState(state)
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

    default:
      log('Unknown message')
      return state
  }
}

interface MaelstromNode {
  id: string
}

interface State {
  node: MaelstromNode
}

let state: State | undefined

// stdin is paused by default
process.stdin.resume()
process.stdin.on('data', (data) => {
  if (data) {
    state = handle(JSON.parse(data.toString()), state)
  }
})
