import { TransactionRegisterKey } from '../lib'
import {
  TransactionId,
  TransactionOperationIndex,
} from '../workloads/transactions'

// TODO: resolve the casting from string to number when accessing records by key

/**
 * Returns true if the graph has a cycle
 *
 * @param sources - Graph nodes with a 0 in-degree
 * @param inDegrees - Map of in-degree per graph node
 * @param graph - A direct serialization graph
 */
function hasCycle(
  sources: Array<TransactionId>,
  inDegrees: Record<TransactionId, number>,
  graph: Record<TransactionId, Array<TransactionId>>
): boolean {
  if (sources.length === 0) {
    return Object.keys(inDegrees).length === 0
  }

  const zeroInDegree: Array<TransactionId> = []
  sources.forEach((txid) => {
    graph[Number(txid)].forEach((dependentTxid) => {
      inDegrees[dependentTxid] -= 1

      if (inDegrees[dependentTxid] === 0) {
        zeroInDegree.push(dependentTxid)
      }
    })

    delete inDegrees[Number(txid)]
  })

  return hasCycle(zeroInDegree, inDegrees, graph)
}

/**
 * Returns true if the dependencies between transactions are serializable (no cycle)
 *
 * @param - Array representing dependencies between pairs of transactions
 */
function isSerializable(
  dependencies: Array<[TransactionId, TransactionId]>
): boolean {
  const nodes: Array<TransactionId> = Array.from(
    dependencies
      .reduce((set: Set<TransactionId>, dependency) => {
        set.add(dependency[0])
        set.add(dependency[1])

        return set
      }, new Set() as Set<number>)
      .values()
  )

  const graph: Record<TransactionId, Array<TransactionId>> = {}
  const inDegrees: Record<TransactionId, number> = {}

  for (let i = 0; i < nodes.length; i++) {
    graph[nodes[i]] = []
    inDegrees[nodes[i]] = 0
  }

  for (let i = 0; i < dependencies.length; i++) {
    const [t1, t2] = dependencies[i]

    if (t2 === t1) {
      continue
    }

    inDegrees[t2] += 1
    graph[t1].push(t2)
  }

  const sources = Object.keys(inDegrees)
    .filter((txid) => inDegrees[Number(txid)] === 0)
    .map(Number)
  return hasCycle(sources, inDegrees, graph)
}

/**
 * Returns trus if the database operations have a write-write cycle
 *
 * @params - Database operations by in-flight transactions
 */
export function hasWriteWrite(
  operations: Array<
    [TransactionId, TransactionOperationIndex, TransactionRegisterKey]
  >
): boolean {
  const lastWritesPerRegister: Record<
    TransactionRegisterKey,
    [TransactionOperationIndex, TransactionId]
  > = {}

  const dependencies: Array<[TransactionId, TransactionId]> = []

  operations.forEach(([txid, txopindex, txregister]) => {
    if (lastWritesPerRegister[txregister] !== undefined) {
      dependencies.push([txid, lastWritesPerRegister[txregister][1]])
    }

    lastWritesPerRegister[txregister] = [txopindex, txid]
  })

  return !isSerializable(dependencies)
}

// export function hasReadDependency(
//   transactionId: TransactionId,
//   registerKey: TransactionRegisterKey,
//   _operations: Array<
//     [
//       TransactionId,
//       TransactionOperationIndex,
//       TransactionRegisterKey,
//       TransactionOperation
//     ]
//   >
// ): { dependency: TransactionId | undefined } {
//   const nodes: Array<TransactionId> = Array.from(
//     transactionId
//       .reduce((set: Set<TransactionId>, operation) => {
//         const transactionId = operation[0]

//         set.add(transactionId)

//         return set
//       }, new Set() as Set<number>)
//       .values()
//   )

//   const graph = {}

//   for (const transactionId of nodes) {
//     graph[transactionId]
//   }

// }
