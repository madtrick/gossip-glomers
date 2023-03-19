
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

function handle(
  message: Message<MessageBodyInit | MessageBodyEcho | MessageBodyGenerate>,
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

    case MessageType.Generate:
      log('Handle generate')
      assertState(state)
      send({
        dest: message.src,
        src: state.node.id,
        body: {
          type: MessageType.GenerateOk,
          in_reply_to: message.body.msg_id,
          id: Math.random()
        }
      })
      return state

    default:
      log('Unknown message')
      return state
  }
}

// stdin is paused by default
process.stdin.resume()
process.stdin.on('data', (data) => {
  if (data) {
    const message = data.toString().trim()
    log(['recv(raw)', data])

    if (message.length > 0) {
      state = handle(JSON.parse(data.toString()), state)
    }
  }
})
