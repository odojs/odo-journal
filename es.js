const equal = require('fast-deep-equal')

module.exports = (db, opts) => {
  if (opts == null) opts = {}

  const sep = opts.sep || '!'
  const prefix = opts.prefix ? sep + opts.prefix + sep : ''

  const key = (store, id) => `${prefix}${store}${sep}${id}`
  const start = (store) => `${prefix}${store}${sep}\x00`
  const end = (store) => `${prefix}${store}${sep}\xff`

  const loadall = (keys, cb) => {
    const now = Date.now()
    const result = {}
    let index = 0
    const iterate = () => {
      if (index >= keys.length) return cb(null, result)
      db.get(keys[index], (err, value) => {
        let data = null
        try {
          if (!err) data = JSON.parse(value.toString('utf8'))
          else if (err.type != 'NotFoundError') return cb(err)
          if (data && data.ttl && data.ttl < now) {
            data = null
            db.del(keys[index])
          }
          result[keys[index]] = data
          index++
          iterate()
        } catch (err) {
          return cb(err)
        }
      })
    }
    iterate()
  }

  return {
    get: (store, id) => new Promise((resolve, reject) => {
      const now = Date.now()
      db.get(key(store, id), (err, value) => {
        let data = null
        try {
          if (!err) data = JSON.parse(value.toString('utf8'))
          else if (err.type != 'NotFoundError') return resolve(err)
          if (data && data.ttl && data.ttl < now) db.del(key(store, id))
          else resolve(data)
        } catch (err) {
          return reject(err)
        }
      })
    }),
    all: (store) => new Promise((resolve, reject) => {
      const now = Date.now()
      const result = {}
      db
        .createReadStream({ gt: start(store), lt: end(store) })
        .on('data', (data) => {
          let id = data.key.toString('utf8')
          id = id.slice(id.lastIndexOf(sep) + 1)
          let value = null
          try {
            value = JSON.parse(data.value.toString('utf8'))
            if (value && value.ttl && value.ttl < now) db.del(data.key)
            else result[id] = value
          } catch (err) { }
        })
        .on('error', reject)
        .on('end', () => resolve(result))
    }),
    batch: (ops, seq) => new Promise((resolve, reject) => {
      const now = Date.now()
      const deletes = {}
      const puts = {}
      let index = ops.length
      while (index--)
      {
        const op = ops[index]
        const k = key(op.store, op.id)
        if (deletes[k]) ops[index] = null
        else if (op.type == 'del' || op.value.ttl < now) deletes[k] = true
        else if (puts[k]) {
          if (op.value.rev && puts[k].rev && puts[k].rev < op.value.rev)
            puts[k] = op.value
          else
            puts[k] = Object.assign(op.value, puts[k])
        }
        else puts[k] = op.value
      }
      loadall(Object.keys(puts), (err, results) => {
        if (err != null) return reject(err)
        for (let k of Object.keys(results)) {
          delete deletes[k]
          const result = results[k]
          if (!result) continue
          if (result.rev && puts[k].rev && puts[k].rev < result.rev)
            delete puts[k]
          else
            puts[k] = Object.assign(result, puts[k])
        }
        const dbops = []
        for (let k of Object.keys(deletes))
          dbops.push({ type: 'del', key: k })
        for (let k of Object.keys(puts))
          dbops.push({ type: 'put', key: k, value: JSON.stringify(puts[k]) })
        if (dbops.length == 0) return resolve(ops)
        if (seq) for (let id of Object.keys(seq))
          dbops.push({ type: 'put', key: key('__seq', id),
            value: JSON.stringify({ seq: seq[id] }) })
        db.batch(dbops).then(() => resolve(ops))
      })
    })
  }
}
module.exports.diff = (store, source, target) => {
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
      else if (!equal(s[key], t[key])) d[key] = t[key]
    if (Object.keys(d) > 0)
      ops.push({ type: 'put', store: store, id: id, value: d })
  }
  for (let id of Object.keys(target)) if (!source[id])
    ops.push({ type: 'add', store: store, id: id, value: target[id] })
  return ops
}
