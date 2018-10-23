const EventEmitter = require('events')

module.exports = (opts) => {
  const logs = opts.logs
  const snapshots = opts.snapshots
  const result = new EventEmitter()

  const subscriptions = {}
  let journals = null

  logs.list((err, list) => {
    journals = {}
    for (let log of list) journals[log.id] = {
      id: log.id,
      from: log.from,
      to: log.to
    }
    const cbs = onopen.splice(0, onopen.length)
    for (let cb of cbs) cb()
  })

  const onopen = []
  const open = (cb) => {
    if (journals) return cb()
    onopen.push(cb)
  }

  result.open = open
  result.list = (cb) => open((err) => {
    if (err != null) return cb(err)
    cb(null, Object.values(journals).map((o) => Object.assign({}, o)))
  })
  result.close = () => {
    for (let collection of Object.values(subscriptions))
      for (let subscription of collection) subscription.close()
    for (let key of Object.keys(subscriptions)) delete subscriptions[key]
  }
  result.snapshot = (id, snapshot, to, cb) => {
    if (!cb) cb = () => {}
    snapshots.put(id, JSON.stringify({ snapshot: snapshot, to: to }), (err) => {
      if (err != null) return cb(err)
      journals[id].snapshotseq = to
      logs.delbatch(id, to, (err) => {
        if (err != null) return cb(err)
        info.from = to + 1
        journals[id].from = to + 1
        result.emit('journal.newsnapshot',
          { id: id, snapshot: snapshot, seq: to })
        if (subscriptions[id]) for (let subscription of subscriptions[id])
          subscription.emit('journal.newsnapshot',
            { snapshot: snapshot, seq: to })
        cb()
      })
    })
  }
  result.append = (id, events, cb) => {
    logs.append(id, events, (err) => {
      if (err && cb) return cb(err)
      if (!journals[id]) journals[id] = {
        id: id,
        from: 1,
        to: 0,
        snapshotseq: null
      }
      //console.log(id, `journaling ${journals[id].to + 1} -> ${journals[id].to + events.length}`)
      journals[id].to += events.length
      result.emit('journal.append', {
        id: id,
        to: journals[id].to,
        events: events
      })
      if (cb) cb()
    })
  }
  result.live = (opts) => {
    const id = opts.id
    const from = opts.from || 1
    const live = new EventEmitter()

    let subscription = null
    if (!subscriptions[id]) subscriptions[id] = []
    const info = {
      id: id,
      loadedfrom: from,
      snapshotseq: null,
      from: null,
      to: null
    }

    logs.info(id, (err, i) => {
      if (i != null) {
        info.from = i.from
        info.to = i.to
        if (journals[id] == null)
          journals[id] = {
            id: id,
            from: null,
            to: null,
            snapshotseq: null
          }
      }
      snapshots.get(id, (err, snapshot) => {
        if (!err) {
          try { snapshot = JSON.parse(snapshot.toString('utf8')) }
          catch (e) { err = e }
        }

        if (err) {
          live.emit('journal.ready', info)
          subscription = logs.live(id, from, (events) => {
            live.emit('journal.events', events)
            for (let e of events) live.emit(e.event.e, e.event.p)
          })
          subscriptions[id].push(live)
          return
        }

        snapsnot.id = id
        journals[id].snapshotseq = snapshot.to
        live.emit('journal.ready', info)

        if (snapshot.to >= from) {
          live.emit('journal.restoresnapshot', snapshot)
          subscription = logs.live(id, snapshot.to + 1, (events) => {
            live.emit('journal.events', events)
            for (let e of events) live.emit(e.event.e, e.event.p)
          })
          subscriptions[id].push(live)
          return
        }

        subscription = logs.live(id, from, (events) => {
          live.emit('journal.events', events)
          for (let e of events) live.emit(e.event.e, e.event.p)
        })
        subscriptions[id].push(live)
      })
    })
    live.close = () => {
      if (!subscription) return
      subscription.close()
      const index = subscriptions[id].indexOf(live)
      if (index > -1) subscriptions[id].splice(index, 1)
    }
    return live
  }
  result.firehose = (seq) => {
    const firehose = new EventEmitter()

    const livesubscriptions = {}

    const subscribe = (id) => {
      if (livesubscriptions[id]) return
      const from = seq && seq[id] ? seq[id] : 1
      const subscription = result.live({ id: id, from: from })
      subscription.on('journal.ready', (info) => {
        firehose.emit('journal.ready', info)
      })
      subscription.on('journal.restoresnapshot', (snapshot) => {
        firehose.emit('journal.restoresnapshot', snapshot)
      })
      subscription.on('journal.events', (events) => {
        firehose.emit('journal.events', events)
      })
      livesubscriptions[id] = subscription
    }

    result.on('journal.append', (e) => subscribe(e.id))
    result.on('journal.newsnapshot', (e) => subscribe(e.id))
    result.list((err, journals) => journals.forEach((j) => subscribe(j.id)))
    firehose.close = () => {
      for (let id of Object.keys(livesubscriptions)) {
        livesubscriptions[id].close()
        delete livesubscriptions[id]
      }
    }
    return firehose
  }
  result.toJSON = () => Object.values(journals)

  return result
}
