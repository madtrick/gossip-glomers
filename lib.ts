export enum MessageType {
  Init = 'init',
  InitOk = 'init_ok',
  Echo = 'echo',
  EchoOk = 'echo_ok',
  Generate = 'generate',
  GenerateOk = 'generate_ok',
  Broadcast = 'broadcast',
  BroadcastOk = 'broadcast_ok',
  Read = 'read',
  ReadOk = 'read_ok',
  Topology = 'topology',
  TopologyOk = 'topology_ok',
  Deliver = 'deliver',
}

type MaelstromNodeId = string
type MessageId = number

export interface MessageBodyInit {
  type: MessageType.Init
  msg_id: MessageId
  node_id: MaelstromNodeId
}

export interface MessageBodyEcho {
  type: MessageType.Echo
  msg_id: MessageId
  echo: string
}

export interface MessageBodyGenerate {
  type: MessageType.Generate
  msg_id: MessageId
}

export interface MessageBodyTopology {
  type: MessageType.Topology
  msg_id: MessageId
  topology: Record<MaelstromNodeId, Array<MaelstromNodeId>>
}

export interface MessageBodyBroadcast {
  type: MessageType.Broadcast
  msg_id: MessageId
  message: number
}

export interface MessageBodyRead {
  type: MessageType.Read
  msg_id: MessageId
  message: number
}

export interface MessageBodyDeliver {
  type: MessageType.Deliver
  message: number
}

export interface Message<B> {
  src: string
  dest: string
  body: B
}

export const log = (data: unknown) =>
  console.error(`[${Date.now()}]: ${JSON.stringify(data)}`)

export function send(data: unknown): void {
  log(['send', data])
  console.log(JSON.stringify(data))
}

export interface MaelstromNode {
  id: string
}

export interface State {
  node: MaelstromNode
  broadcast: {
    neighbours: Array<MaelstromNodeId>
    messages: Array<number>
  }
}