import {
  ConsoleInputChannel,
  ConsoleOutputchannel,
  KakfkaLogKey,
  KakfkaLogOffset,
  Message,
  MessageBodyInit,
  MessageBodyKVReadOk,
  MessageBodyKafkaPoll,
  MessageType,
  MessageBodyKafkaCommitOffsets,
  MessageBodyKafkaSend,
  MessageBodyKafkaListCommittedOffsets,
  isErrorMessage,
  ErrorTypes,
  KafkaLog,
} from '../lib'
import { ANode } from '../node'

type State = unknown

const LIN_KVID = 'lin-kv'
const SEQ_KVID = 'seq-kv'

const input = new ConsoleInputChannel()
const output = new ConsoleOutputchannel()
const node = new ANode<State>({}, input, output)

node.on(MessageType.Init, async (node, _state, message) => {
  const initMessage = message as Message<MessageBodyInit>
  const {
    body: { node_id: nodeId },
  } = initMessage

  node.id = nodeId
  node.send(message.src, {
    type: MessageType.InitOk,
    in_reply_to: initMessage.body.msg_id,
  })
})

node.on(MessageType.Send, async (node, _state, message) => {
  const {
    src,
    body: { key, msg, msg_id },
  } = message as Message<MessageBodyKafkaSend>

  const reply = await node.rpc(SEQ_KVID, {
    type: MessageType.KVRead,
    key,
  })
  let currentOffset: KakfkaLogOffset
  let currentLog: KafkaLog

  if (isErrorMessage(reply)) {
    if (reply.body.code === ErrorTypes.KeyDoesNotExist) {
      await node.rpc(SEQ_KVID, {
        type: MessageType.KVCas,
        key,
        to: [],
        create_if_not_exists: true,
      })

      currentOffset = 0
      currentLog = []
    } else {
      throw new Error('Unhandled error')
    }
  } else {
    const readMessage = reply as Message<MessageBodyKVReadOk<KafkaLog>>
    currentLog = readMessage.body.value
    currentOffset = currentLog.length
  }

  let offset = currentOffset
  let casReply = await node.rpc(SEQ_KVID, {
    type: MessageType.KVCas,
    key,
    from: currentLog,
    to: [...currentLog, msg],
  })

  while (isErrorMessage(casReply)) {
    const reply = await node.rpc(SEQ_KVID, {
      type: MessageType.KVRead,
      key,
    })
    const readMessage = reply as Message<MessageBodyKVReadOk<KafkaLog>>
    currentLog = readMessage.body.value
    casReply = await node.rpc(SEQ_KVID, {
      type: MessageType.KVCas,
      key,
      from: currentLog,
      to: [...currentLog, msg],
    })

    offset = currentLog.length
  }

  node.send(src, {
    type: MessageType.SendOk,
    offset,
    in_reply_to: msg_id,
  })
})

node.on(MessageType.Poll, async (_node, _state, message) => {
  const {
    src,
    body: { offsets, msg_id },
  } = message as Message<MessageBodyKafkaPoll>

  const messages: Record<KakfkaLogKey, Array<[KakfkaLogOffset, number]>> = {}

  for (const key in offsets) {
    const offset = offsets[key]
    const reply = await node.rpc(SEQ_KVID, {
      type: MessageType.KVRead,
      key,
    })

    if (isErrorMessage(reply)) {
      if (reply.body.code === ErrorTypes.KeyDoesNotExist) {
        continue
      } else {
        throw new Error('Unhandled error')
      }
    } else {
      const readMessage = reply as Message<MessageBodyKVReadOk<KafkaLog>>
      const values = readMessage.body.value

      values.reduce((acc, message, index) => {
        if (index < offset) {
          return acc
        }

        if (acc[key] === undefined) {
          acc[key] = []
        }

        acc[key].push([index, message])

        return acc
      }, messages)
    }
  }

  node.send(src, {
    type: MessageType.PollOk,
    msgs: messages,
    in_reply_to: msg_id,
  })
})

node.on(MessageType.CommitOffsets, async (node, _state, message) => {
  const {
    src,
    body: { msg_id, offsets },
  } = message as Message<MessageBodyKafkaCommitOffsets>

  for (const key in offsets) {
    const offset = offsets[key]

    await node.rpc(LIN_KVID, {
      // TODO: use `cas`
      type: MessageType.KVWrite,
      key: `${key}-committed-offset`,
      value: offset,
    })
  }

  node.send(src, {
    type: MessageType.CommitOffsetsOk,
    in_reply_to: msg_id,
  })
})

node.on(MessageType.ListCommittedOffsets, async (node, _state, message) => {
  const {
    src,
    body: { msg_id, keys },
  } = message as Message<MessageBodyKafkaListCommittedOffsets>

  const offsets: Record<KakfkaLogKey, KakfkaLogOffset> = {}

  for (const key of keys) {
    const reply = await node.rpc(LIN_KVID, {
      type: MessageType.KVRead,
      key: `${key}-committed-offset`,
    })

    if (isErrorMessage(reply)) {
      if (reply.body.code === ErrorTypes.KeyDoesNotExist) {
        offsets[key] = 0
      } else {
        throw new Error('Unhandled error')
      }
    } else {
      const readMessage = message as Message<
        MessageBodyKVReadOk<KakfkaLogOffset>
      >

      offsets[key] = readMessage.body.value
    }
  }

  node.send(src, {
    type: MessageType.ListCommittedOffsetsOk,
    offsets,
    in_reply_to: msg_id,
  })
})
