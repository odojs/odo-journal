const lexint = require('lexicographic-integer')
const through = require('through2')
const pump = require('pump')

module.exports = (db, opts) => {
  if (opts == null) opts = {}

  const sep = opts.sep || '!'
  const prefix = opts.prefix ? sep + opts.prefix + sep : ''

  const key = (id, seq) =>
    `${prefix}${id}${sep}${seq === -1 ? '\xff' : lexint.pack(seq, 'hex')}`

  let logs = null
  const list = (cb) => {
    if (logs != null) return cb(null, logs)
    const result = {}

    let end = `${prefix}\xff`
    let found = false

    const iterate = () => {
      db
        .createKeyStream({gt: prefix, lt: end, reverse: true, limit: 1})
        .on('data', (data) => {
          data = data.toString('utf8')
          const id = data.slice(prefix.length, data.lastIndexOf(sep))
          const to = lexint.unpack(data.slice(data.lastIndexOf(sep) + 1), 'hex')
          result[id] = { id: id, to: to }
          found = id
        })
        .on('error', (err) => cb)
        .on('end', () => {
          if (!found) {
            logs = result
            cb(null, Object.values(result).map((o) => Object.assign({}, o)))
            return
          }
          db
            .createKeyStream({gt: prefix, lt: end, limit: 1})
            .on('data', (data) => {
              data = data.toString('utf8')
              const from = lexint.unpack(data.slice(data.lastIndexOf(sep) + 1), 'hex')
              result[found].from = from
            })
            .on('error', (err) => cb)
            .on('end', () => {
              end = key(found, 0)
              found = false
              iterate()
            })
        })
    }
    iterate()
  }

  let isopen = false
  let isopening = false
  const onopen = []
  const open = (cb) => {
    if (isopen) return cb()
    onopen.push(cb)
    if (isopening) return
    isopening = true
    list((err) => {
      isopen = true
      isopening = false
      const cbs = onopen.splice(0, onopen.length)
      for (let cb of cbs) cb(err)
      return
    })
  }

  const createReadStream = (id, opts) => {
    if (!opts) opts = {}

    const rs = db.createReadStream({
      gte: key(id, opts.from || 0),
      lte: key(id, opts.to || -1),
      keyEncoding: 'utf8',
      valueEncoding: 'utf8',
      reverse: opts.reverse,
      limit: opts.limit
    })

    const format = through.obj((data, enc, cb) => {
      const key = data.key.toString('utf8')
      const id = key.slice(prefix.length, key.lastIndexOf(sep))
      const seq = lexint.unpack(key.slice(prefix.length + id.length + 1), 'hex')
      try { data.value = JSON.parse(data.value.toString('utf8')) }
      catch (e) { data.value = null }
      cb(null, { id: id, seq: seq, event: data.value })
    })

    return pump(rs, format)
  }

  const logsubscribers = {}

  return {
    open: open,
    key: key,
    list: list,
    info: (id, cb) => open((err) => {
      if (err != null) return cb(err)
      cb(null, logs[id])
    }),
    close: (cb) => {
      isopen = false
      isopening = false
      onopen.splice(0, onopen.length)
      logs = null
      db.close(cb)
    },
    isOpen: () => db.isOpen,
    isClosed: () => db.isClosed,
    put: (id, seq, event, cb) => open((err) => {
      if (err != null) return cb(err)
      db.put(key(id, seq), JSON.stringify(event), cb)
    }),
    get: (id, seq, cb) => open((err) => {
      if (err != null) return cb(err)
      db.get(key(id, seq), cb)
    }),
    del: (id, seq, cb) => open((err) => {
      if (err != null) return cb(err)
      db.del(key(id, seq), cb)
    }),
    delbatch: (id, to, cb) => open((err) => {
      if (err != null) return cb(err)
      const log = logs[id]
      const ops = [...Array(to - log.from + 1).keys()]
        .map(x => { return { type: 'del', key: key(id, x + log.from) } })
      db.batch(ops, (err) => {
        if (err != null) return cb(err)
        log.from = to + 1
        cb()
      })
    }),
    batch: (array, cb) => open((err) => {
      if (err != null) return cb(err)
      db.batch(array.map((o) => {
        return {
          type: o.type,
          key: key(o.id, o.seq),
          value: JSON.stringify(o.value)
        }
      }), cb)
    }),
    append: (id, events, cb) => open((err) => {
      if (err != null) return cb(err)
      if (!logs[id]) logs[id] = { id: id, from: 1, to: 0 }
      const log = logs[id]
      const payloads = events.map((e) => {
        return { id: id, seq: ++log.to, event: e }
      })
      const operations = payloads.map((p) => {
        return { type: 'put', key: key(id, p.seq), value: JSON.stringify(p.event) }
      })
      db.batch(operations, (err) => {
        if (err != null) return cb(err)
        if (logsubscribers[id] != null)
          for (let subscriber of logsubscribers[id])
            subscriber(payloads)
        cb()
      })
    }),
    live: (id, from, cb) => {
      let issubscribed = true
      if (!logsubscribers[id]) logsubscribers[id] = []

      // record everything happening from now
      let queue = []
      const enqueue = (events) => {
        for (let e of events) queue.push(e)
      }
      if (!logs || !logs[id] || from <= logs[id].to) logsubscribers[id].push(enqueue)
      let seq = from - 1

      let live = null

      let events = []
      let rs = createReadStream(id, { from: from })
        .on('data', (data) => {
          seq = data.seq
          events.push(data)
          if (events.length > 1000) {
            cb(events)
            events = []
          }
        })
        .on('end', () => {
          if (!issubscribed) return
          if (events.length > 0) cb(events)
          events = []
          if (rs != null) rs.end()
          const queueindex = logsubscribers[id].indexOf(enqueue)
          if (queueindex > -1) logsubscribers[id].splice(queueindex, 1)
          live = cb
          logsubscribers[id].push(live)
          queue = queue.filter((e) => e.seq > seq)
          if (queue.length > 0) cb(queue)
          queue = []
        })


      return {
        close: () => {
          issubscribed = false
          if (rs != null) rs.end()
          const queueindex = logsubscribers[id].indexOf(enqueue)
          if (queueindex > -1) logsubscribers[id].splice(queueindex, 1)
          queue = []
          const liveindex = logsubscribers[id].indexOf(live)
          if (liveindex > -1) logsubscribers[id].splice(liveindex, 1)
        }
      }
    },
    createValueStream: (id, opts) => {
      if (!opts) opts = {}

      const rs = db.createValueStream({
        gte: key(id, opts.from || 0),
        lte: key(id, opts.to || -1),
        keyEncoding: 'utf8',
        valueEncoding: 'utf8',
        reverse: opts.reverse,
        limit: opts.limit
      })

      const format = through.obj((data, enc, cb) => {
        try { data = JSON.parse(data.toString()) }
        catch (e) { data = null }
        cb(null, data)
      })

      return pump(rs, format)
    },
    createReadStream: createReadStream
  }
}
