enum MessageType {
  Init = 'init',
  InitOk = 'init_ok',
  Echo = 'echo',
  EchoOk = 'echo_ok',
  Generate = 'generate',
  GenerateOk = 'generate_ok'
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

interface MessageBodyGenerate {
  type: MessageType.Generate
  msg_id: number
}

interface Message<B> {
  src: string
  dest: string
  body: B
}

const log = (data: any) =>
  console.error(`[${Date.now()}]: ${JSON.stringify(data)}`)

function send(data: any): void {
  log(['send', data])
  console.log(JSON.stringify(data))
}

interface MaelstromNode {
  id: string
}

interface State {
  node: MaelstromNode
}

let state: State | undefined
