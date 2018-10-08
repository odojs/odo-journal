module.exports = (db, opts) => {
  if (opts == null) opts = {}

  const sep = opts.sep || '!'
  const prefix = opts.prefix ? sep + opts.prefix + sep : ''

  const key = (store, id) => `${prefix}${store}${sep}${id}`
  const start = (store) => `${prefix}${store}${sep}\x00`
  const end = (store) => `${prefix}${store}${sep}\xff`

  const loadall = (keys, cb) => {
    const result = {}
    let index = 0
    const iterate = () => {
      if (index >= keys.length) return cb(null, result)
      db.get(keys[index], (err, value) => {
        let data = null
        try {
          if (!err) data = JSON.parse(value.toString('utf8'))
          else if (err.type != 'NotFoundError') return cb(err)
        } catch (err) {
          return cb(err)
        }
        result[keys[index]] = data
        index++
        iterate()
      })
    }
    iterate()
  }

  return {
    get: (store, id, cb) => {
      db.get(key(store, id), (err, value) => {
        let data = null
        try {
          if (!err) data = JSON.parse(value.toString('utf8'))
          else if (err.type != 'NotFoundError') return cb(err)
        } catch (err) {
          return cb(err)
        }
        cb(null, data)
      })
    },
    all: (store, cb) => {
      const result = {}
      db
        .createReadStream({ gt: start(store), lt: end(store) })
        .on('data', (data) => {
          let id = data.key.toString('utf8')
          id = id.slice(id.lastIndexOf(sep) + 1)
          let value = null
          try {
            value = JSON.parse(data.value.toString('utf8'))
          } catch (err) { }
          result[id] = value
        })
        .on('error', cb)
        .on('end', () => {
          cb(null, result)
        })
    },
    batch: (ops, cb) => {
      const deletes = {}
      const puts = {}
      let index = ops.length
      while (index--)
      {
        const op = ops[index]
        const k = key(op.store, op.id)
        if (deletes[k])
          ops[index] = null
        else if (op.type == 'del')
          deletes[k] = true
        else if (puts[k])
          puts[k] = Object.assign(op.value, puts[k])
        else
          puts[k] = op.value
      }
      loadall(Object.keys(puts), (err, results) => {
        if (err != null) return cb(err)
        for (let k of Object.keys(results)) {
          delete deletes[k]
          const result = results[k]
          if (!result) continue
          puts[k] = Object.assign(result, puts[k])
        }
        ops = []
        for (let k of Object.keys(deletes))
          ops.push({ type: 'del', key: k })
        for (let k of Object.keys(puts))
          ops.push({ type: 'put', key: k, value: JSON.stringify(puts[k]) })
        console.log(ops)
        db.batch(ops, cb)
      })
    }
  }
}
