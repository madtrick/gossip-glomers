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
  DeliverOk = 'deliver_ok',
  KVRead = 'read',
  KVReadOk = 'read_ok',
  KVWrite = 'write',
  KVWriteOk = 'write_ok',
  KVCas = 'cas',
  KVCasOk = 'cas_ok',
  Add = 'add',
  AddOk = 'add_ok',
  Error = 'error',
  Send = 'send',
  SendOk = 'send_ok',
  Poll = 'poll',
  PollOk = 'poll_ok',
  CommitOffsets = 'commit_offsets',
  CommitOffsetsOk = 'commit_offsets_ok',
  ListCommittedOffsets = 'list_committed_offsets',
  ListCommittedOffsetsOk = 'list_committed_offsets_ok',
  Txn = 'txn',
  TxnOK = 'txn_ok',
}

export enum ErrorTypes {
  KeyDoesNotExist = 20,
  PreconditionFailed = 22,
}

export function assertMessageType<T extends MessageType>(
  message: Message,
  type: T
): asserts message is Message<TypableMessage<T>> {
  if (message.body.type !== type) {
    throw new Error('Unexpected message type')
  }
}

export type MaelstromNodeId = string
export type MessageId = number
export type KVKey = string

export interface TypableMessage<T = MessageType> {
  type: T
}

export interface ReplyMessage {
  in_reply_to: MessageId
}

export interface MessageBodyInit extends TypableMessage<MessageType.Init> {
  msg_id: MessageId
  node_id: MaelstromNodeId
  node_ids: Array<MaelstromNodeId>
}

export interface MessageBodyEcho extends TypableMessage<MessageType.Echo> {
  msg_id: MessageId
  echo: string
}

export interface MessageBodyGenerate
  extends TypableMessage<MessageType.Generate> {
  msg_id: MessageId
}

export interface MessageBodyTopology
  extends TypableMessage<MessageType.Topology> {
  msg_id: MessageId
  topology: Record<MaelstromNodeId, Array<MaelstromNodeId>>
}

export interface MessageBodyBroadcast
  extends TypableMessage<MessageType.Broadcast> {
  msg_id: MessageId
  message: number
}

export interface MessageBodyRead extends TypableMessage<MessageType.Read> {
  msg_id: MessageId
  message: number
}

export interface MessageBodyDeliver
  extends TypableMessage<MessageType.Deliver> {
  msg_id: MessageId
  message: number
  broadcast_to: MaelstromNodeId
  broadcast_message_id: MessageId
}

export interface MessageBodyDeliverOk
  extends TypableMessage<MessageType.DeliverOk>,
    ReplyMessage {}

export function messageIsDeliverOk(
  message: Message
): message is Message<MessageBodyDeliverOk> {
  return message.body.type === MessageType.DeliverOk
}

export interface MessageBodyKVRead extends TypableMessage<MessageType.KVRead> {
  key: KVKey
}

export interface MessageBodyKVReadOk<T>
  extends TypableMessage<MessageType.KVReadOk> {
  value: T
}

export interface MessageBodyKVWrite
  extends TypableMessage<MessageType.KVWrite> {
  key: KVKey
  value: unknown
}

export interface MessageBodyAdd extends TypableMessage<MessageType.Add> {
  delta: number
  msg_id: MessageId
}

export type KakfkaLogKey = string
export type KakfkaLogOffset = number
export type KafkaLogValue = number
export type KafkaLog = Array<KafkaLogValue>

export interface MessageBodyKafkaSend extends TypableMessage<MessageType.Send> {
  key: KakfkaLogKey
  msg: KafkaLogValue
  msg_id: MessageId
}

export interface MessageBodyKafkaPoll extends TypableMessage<MessageType.Poll> {
  offsets: Record<KakfkaLogKey, KakfkaLogOffset>
  msg_id: MessageId
}

export interface MessageBodyKafkaCommitOffsets
  extends TypableMessage<MessageType.CommitOffsets> {
  offsets: Record<KakfkaLogKey, KakfkaLogOffset>
  msg_id: MessageId
}

export interface MessageBodyKafkaListCommittedOffsets
  extends TypableMessage<MessageType.ListCommittedOffsets> {
  keys: Array<KakfkaLogKey>
  msg_id: MessageId
}

export enum TransactionOperation {
  Read = 'r',
  Write = 'w',
}
export type TransactionRegisterKey = number
export type TransactionRegisterValue = number
export type TransactionAction = [
  TransactionOperation,
  TransactionRegisterKey,
  TransactionRegisterValue
]
export interface MessageBodyTxn extends TypableMessage<MessageType.Txn> {
  txn: Array<TransactionAction>
  msg_id: MessageId
}

export function isErrorMessage(
  message: Message<TypableMessage>
): message is Message<MessageBodyError> {
  return message.body.type === MessageType.Error
}

export interface MessageBodyError
  extends TypableMessage<MessageType.Error>,
    ReplyMessage {
  code: ErrorTypes
  text: string
}

export interface Message<B = TypableMessage> {
  src: string
  dest: string
  body: B
}

type MessageTimeout = NodeJS.Timeout

export interface OutstandingMessage {
  timeout: MessageTimeout
}

export const log = (data: unknown) =>
  console.error(`[${new Date().toISOString()}]: ${JSON.stringify(data)}`)

export type MessageHandler<State> = (
  node: MaelstromNode<State>,
  state: State,
  message: Message<TypableMessage>
) => void

export interface MaelstromNode<State> {
  id: string
  neighbours: Array<MaelstromNodeId>
  state: State
  on: (type: MessageType, handler: MessageHandler<State>) => void
  send: (dest: MaelstromNodeId, message: Record<string, unknown>) => void
  rpc: (
    dest: MaelstromNodeId,
    message: Record<string, unknown>,
    callback?: MessageHandler<State>
  ) => void
  rpcSync: (
    dest: MaelstromNodeId,
    message: Record<string, unknown>
  ) => Promise<Message<TypableMessage>>
}

export interface State {
  outstandingMessages: Record<MessageId, OutstandingMessage>
  broadcast: {
    neighbours: Array<MaelstromNodeId>
    messages: Array<number>
  }
}

export interface InputChannel {
  attach: (callback: (data: unknown) => void) => void
}

export class ConsoleInputChannel implements InputChannel {
  private callback: (data: unknown) => void

  constructor() {
    process.stdin.resume()
    process.stdin.on('data', (data) => {
      if (data) {
        // log(['recv(string)', data.toString()])

        const messages = data.toString().trim().split('\n')
        messages.forEach((message) => {
          if (message.length > 0) {
            return this.callback(JSON.parse(message))
          }
        })
      }
    })

    this.callback = () => undefined
  }

  attach(callback: (data: unknown) => void): void {
    this.callback = callback
  }
}

export interface OutputChannel {
  push: (message: Message<unknown>) => void
}

export class ConsoleOutputchannel implements OutputChannel {
  push(message: Message<unknown>): void {
    console.log(JSON.stringify(message))
  }
}
