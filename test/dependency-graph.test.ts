import { TransactionOperation, TransactionRegisterKey } from '../lib'
import { hasReadDependency, hasWriteWrite } from '../lib/dependency-graph'
import {
  TransactionId,
  TransactionOperationIndex,
} from '../workloads/transactions'

describe('foo', () => {
  // eslint-disable-next-line jest/expect-expect
  it('bar', async () => {
    const tx1 = 1
    const tx2 = 2
    const key = 1

    const transactionsOperations: Array<
      [TransactionId, TransactionOperationIndex, TransactionRegisterKey]
    > = [
      [tx1, 0, key],
      [tx2, 1, key],
      [tx1, 2, key],
    ]

    expect(hasWriteWrite(transactionsOperations)).toBe(true)
  })

  it('bar 2', async () => {
    const tx1 = 1
    const tx2 = 2
    const keytx1 = 1
    const keytx2 = 1

    const transactionsOperations: Array<
      [TransactionId, TransactionOperationIndex, TransactionRegisterKey]
    > = [
      [tx1, 0, keytx1],
      [tx2, 1, keytx2],
    ]

    expect(hasWriteWrite(transactionsOperations)).toBe(false)
  })

  it('bar 3', async () => {
    const tx1 = 1
    const keytx1 = 1

    const transactionsOperations: Array<
      [TransactionId, TransactionOperationIndex, TransactionRegisterKey]
    > = [
      [tx1, 0, keytx1],
      [tx1, 1, keytx1],
    ]

    expect(hasWriteWrite(transactionsOperations)).toBe(false)
  })
})

// describe('hasReadDependency', () => {
//   it('foo', () => {
//     const tx1 = 1
//     const keytx1 = 1

//     const transactionsOperations: Array<
//       [
//         TransactionId,
//         TransactionOperationIndex,
//         TransactionRegisterKey,
//         TransactionOperation
//       ]
//     > = [[tx1, 0, keytx1, TransactionOperation.Read]]

//     expect(hasReadDependency(tx1, keytx1, transactionsOperations)).toEqual({
//       dependency: undefined,
//     })
//   })

//   it('bar', () => {
//     const tx1 = 1
//     const tx2 = 2
//     const keytx1 = 1

//     const transactionsOperations: Array<
//       [
//         TransactionId,
//         TransactionOperationIndex,
//         TransactionRegisterKey,
//         TransactionOperation
//       ]
//     > = [
//       [tx2, 0, keytx1, TransactionOperation.Read],
//       [tx1, 1, keytx1, TransactionOperation.Read],
//     ]

//     expect(hasReadDependency(tx1, keytx1, transactionsOperations)).toEqual({
//       dependency: undefined,
//     })
//   })

//   it('bar 2', () => {
//     const tx1 = 1
//     const tx2 = 2
//     const keytx1 = 1

//     const transactionsOperations: Array<
//       [
//         TransactionId,
//         TransactionOperationIndex,
//         TransactionRegisterKey,
//         TransactionOperation
//       ]
//     > = [
//       [tx2, 0, keytx1, TransactionOperation.Write],
//       [tx1, 1, keytx1, TransactionOperation.Read],
//     ]

//     expect(hasReadDependency(tx2, keytx1, transactionsOperations)).toEqual({
//       dependency: tx1,
//     })
//   })
// })
