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
    get: (store, id, cb) => {
      const now = Date.now()
      db.get(key(store, id), (err, value) => {
        let data = null
        try {
          if (!err) data = JSON.parse(value.toString('utf8'))
          else if (err.type != 'NotFoundError') return cb(err)
          if (data && data.ttl && data.ttl < now) db.del(key(store, id))
          else cb(null, data)
        } catch (err) {
          return cb(err)
        }
      })
    },
    all: (store, cb) => {
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
        .on('error', cb)
        .on('end', () => {
          cb(null, result)
        })
    },
    batch: (ops, cb) => {
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
        if (err != null) return cb(err)
        for (let k of Object.keys(results)) {
          delete deletes[k]
          const result = results[k]
          if (!result) continue
          if (result.rev && puts[k].rev && puts[k].rev < result.rev)
            delete puts[k]
          else
            puts[k] = Object.assign(result, puts[k])
        }
        ops = []
        for (let k of Object.keys(deletes))
          ops.push({ type: 'del', key: k })
        for (let k of Object.keys(puts))
          ops.push({ type: 'put', key: k, value: JSON.stringify(puts[k]) })
        db.batch(ops, cb)
      })
    }
  }
}
