const level = require('level-mem')
const esup = require('./es')

const db = level()
const es = esup(db)

const diff = (store, source, target) => {
  const ops = []
  for (let id of Object.keys(source)) {
    const s = source[id]
    if (!target[id]) {
      ops.push({ type: 'del', store: store, id: id })
      continue
    }
    const t = target[id]
    const d = {}
    for (let key of Object.keys(s))
      if (!t[key]) d[key] = null
    for (let key of Object.keys(t))
      if (!s[key]) d[key] = t[key]
      else if (s[key] !== t[key]) d[key] = t[key]
    ops.push({ type: 'put', store: store, id: id, value: d })
  }
  for (let id of Object.keys(target))
    if (!source[id])
      ops.push({ type: 'put', store: store, id: id, value: target[id] })
  return ops
}

es.batch([
  { type: 'put', store: 'people', id: 1, value: { name: 'nancy' } },
  { type: 'put', store: 'people', id: 1, value: { occupation: 'teacher' } },
  { type: 'put', store: 'people', id: 2, value: { name: 'bob' } },
  { type: 'del', store: 'people', id: 2 },
  { type: 'put', store: 'people', id: 3, value: { name: 'bob' } },
], (err) => {
  es.all('people', (err, people) => {
    console.log(diff('people', people, {
      '1': { name: 'judy', occupation: 'teacher' }
    }))
  })
  // es.get('people', 1, (err, person) => {
  //   console.log(person)
  // })
})

// new uuid or next int as buttons for id
// diff field changes for efficiency
// datetime picker for timestamp
// search multi select for multi (using Name if available)
// search single select for single (using Name if available)
// all nullable
// radio select for select
// rev not shown but set with current
// ttl settable for expiry
const def = {
  Vendor: {
    Name: { type: 'string' }
  },
  Tag: {
    Name: { type: 'string' },
    Vendor: { type: 'single' },
    InOrOut: { type: 'select', values: ['In', 'Out'] },
    WeighingType: { type: 'select', values: ['1', 'S', 'N'] },
    CanWeighMultiple: { type: 'boolean' },
    Products: { type: 'multi', store: 'Product' }
  },
  Product: {
    Name: { type: 'string' }
  },
  Tare: {
    Tag: { type: 'single', store: 'Tag' },
    Weight: { type: 'int' },
    ttl: { type: 'ttl' },
    rev: { type: 'rev' }
  },
  Transaction: {
    Tag: { type: 'single', store: 'Tag' },
    Weight1: { type: 'int' },
    Time1: { type: 'timestamp' },
    Weight2: { type: 'int' },
    Time2: { type: 'timestamp' },
    ProductWeight: { type: 'int' },
    InOrOut: { type: 'select', values: ['In', 'Out'] },
    DidUseTare: { type: 'boolean' },
    Product: { type: 'single', store: 'Product' },
    Source: { type: 'single', store: 'Instance' }
  },
  Instance: {
    Configuration: { type: 'string' },
    Protocol: { type: 'string' },
    Device: { type: 'string' }
  }
}
