const seuss = require('seuss-queue')

module.exports = (opts) => {
  const journalup = opts.journals
  const peers = {} // connected peers
  const directory = {} // all known journals
  const mylist = {} // my current state of journals
  const mutex = {} // lock journals during sync
  const incoming = {} // queue of event syncs

  journalup.list((err, journals) => {
    for (let j of journals) {
      mylist[j.id] = j
      if (!directory[j.id]) directory[j.id] = {
        id: j.id,
        peers: {},
        self: j,
        isself: false
      }
    }
    if (!directory[opts.id]) directory[opts.id] = {
      id: opts.id,
      peers: {},
      self: {
        id: opts.id,
        from: 1,
        to: 0,
        snapshotseq: null
      },
      isself: true
    }
    directory[opts.id].isself = true
  })
  journalup.on('journal.append', (j) => {
    if (!mylist[j.id]) mylist[j.id] = {
      id: j.id,
      from: 1,
      to: j.to,
      snapshotseq: null
    }
    mylist[j.id].to = j.to
    if (!directory[j.id]) directory[j.id] = {
      id: j.id,
      peers: {},
      self: mylist[j.id],
      isself: false,
      subscribedto: null
    }
    if (!directory[j.id].self) directory[j.id].self = {
      id: j.id,
      from: 1,
      to: j.to,
      snapshotseq: null
    }
    directory[j.id].self.to = j.to
    //console.log(`sync ${opts.id} append`, j)
  })
  journalup.on('journal.newsnapshot', (s) => {
    mylist[s.id].snapshotseq = s.seq
    directory[s.id].self.snapshotseq = s.seq
    //console.log(`sync ${opts.id} new snapshot`, s)
  })

  const bestpeer = (peers) => {
    let to = 0
    let result = null
    for (let peer of Object.keys(peers)) {
      if (peers[peer].to <= to) return
      to = peers[peer].to
      result = peer
    }
    return result
  }

  const evaluate = () => {
    const plan = []
    for (let j of Object.values(directory)) {
      const peer = bestpeer(j.peers)

      // we don't subscribe to our own data or if there are no peers
      if (j.isself || !peer) {
        if (j.subscribedto)
          plan.push({ op: 'remove', journal: j.id, peer: j.subscribedto })
        continue
      }

      // A peer to subscribe to. We subscribe even if they are behind.
      const from = j.self ? j.self.to + 1 : 1
      if (j.subscribedto) {
        if (j.subscribedto != peer) {
          plan.push({ op: 'remove', journal: j.id, peer: j.subscribedto })
          plan.push({ op: 'add', journal: j.id, peer: peer, from: from })
        }
        continue
      }

      plan.push({ op: 'add', journal: j.id, peer: peer, from: from })
    }
    for (let e of plan) {
      switch (e.op) {
      case 'remove':
        directory[e.journal].subscribedto = null
        if (!peers[e.peer] || !peers[e.peer].incoming[e.journal]) continue
        delete peers[e.peer].incoming[e.journal]
        peers[e.peer].peer.write('sync.unsubscribe', { id: e.journal })
        break
      case 'add':
        directory[e.journal].subscribedto = e.peer
        peers[e.peer].incoming[e.journal] = e.from
        peers[e.peer].peer.write('sync.subscribe', { id: e.journal, from: e.from })
        break
      }
    }
    for (let peer of Object.values(peers))
      peer.peer.write('sync.requestjournals')
  }

  setInterval(evaluate, 10000)

  const enqueue = (id, events) => {
    if (!incoming[id]) incoming[id] = seuss()
    incoming[id].enqueue(() => new Promise((resolve, reject) => {
      let min = +Infinity
      let max = -Infinity
      events.forEach(e => {
        if (e.seq < min) min = e.seq
        if (e.seq > max) max = e.seq
      })
      //console.log(`sync append batch ${id} ${min} -> ${max}`)
      //console.log(directory[id])
      //console.log(events.map((e) => e.event))
      journalup.append(id, events.map((e) => e.event), (err) => {
        if (err) return reject()
        resolve()
      })
    }))
  }

  const drain = () => {
    for (let id of Object.keys(incoming)) {
      const q = incoming[id]
      if (q.length() == 0 || mutex[id]) continue
      mutex[id] = true
      q.dequeue()()
        .then(() => {
          delete mutex[id]
          drain()
        })
        .catch((err) => {
          console.error(err)
          delete mutex[id]
          drain()
        })
    }
  }

  return {
    add: (peer) => {
      peers[peer.id] = { peer: peer, incoming: {}, outgoing: {} }
      peer.on('swarm.ready', () => {
        peer.write('sync.requestjournals')
      })
      peer.on('swarm.disconnect', (reason) => {
        //console.log(`sync peer disconnect ${peer.id} ${peer.key} — ${reason}`)
        if (peers[peer.id]) {
          for (let subscription of Object.values(peers[peer.id].outgoing))
            subscription.close()
          delete peers[peer.id]
        }
        for (let journal of Object.values(directory))
          delete journal.peers[peer.id]
        evaluate()
      })
      peer.on('sync.requestjournals', (e) => {
        peer.write('sync.currentjournals', mylist)
      })
      peer.on('sync.currentjournals', (theirlist) => {
        for (let j of Object.values(theirlist)) {
          if (!directory[j.id]) directory[j.id] = {
            id: j.id,
            peers: {},
            self: null,
            isself: false,
            subscribedto: null
          }
          directory[j.id].peers[peer.id] = j
        }
        evaluate()
      })
      peer.on('sync.subscribe', (journal) => {
        //console.log(`${opts.id} subscribing ${peer.id}`, journal)
        if (peers[peer.id].outgoing[journal.id]) return
        const subscription = journalup.live({ id: journal.id, from: journal.from })
          .on('journal.restoresnapshot', (s) => {
            peer.write('sync.restoresnapshot', { id: journal.id, snapshot: s })
          })
          .on('journal.events', (events) => {
            //console.log(`${opts.id} -> ${peer.id}`, events)
            peer.write('sync.events', events)
          })
          .on('journal.newsnapshot', (s) => {
            peer.write('sync.newsnapshot', { id: journal.id, snapshot: s })
          })
        peers[peer.id].outgoing[journal.id] = subscription
      })
      peer.on('sync.unsubscribe', (journal) => {
        if (!peers[peer.id] || !peers[peer.id].outgoing[journal.id]) return
        //console.log(`${opts.id} unsubscribing ${peer.id}`, journal)
        peers[peer.id].outgoing[journal.id].close()
        delete peers[peer.id].outgoing[journal.id]
      })
      peer.on('sync.restoresnapshot', (s) => {x
        if (mylist[s.id] && mylist[s.id].to > s.snapshot.to) return
        //console.log(`${opts.id} restoresnapshot ${peer.id}`, journal)
        journalup.snapshot(s.id, s.snapshot.snapshot, s.snapshot.to)
      })
      peer.on('sync.events', (events) => {
        if (!peers[peer.id]) return
        //console.log(`${opts.id} receiving events from ${peer.id}`, events)
        const sets = {}
        for (let e of events) {
          if (!peers[peer.id].incoming[e.id]) continue
          if (!sets[e.id]) {
            sets[e.id] = {
              id: e.id,
              seq: directory[e.id].self ? directory[e.id].self.to : 0,
              events: []
            }
          }
          if (sets[e.id].seq + 1 == e.seq) {
            sets[e.id].seq++
            sets[e.id].events.push(e)
          }
        }
        for (let e of Object.values(sets))
          if (e.events.length > 0) enqueue(e.id, e.events)
        drain()
      })
      peer.on('sync.newsnapshot', (s) => {
        if (mylist[s.id] && mylist[s.id].to > s.snapshot.to) return
        //console.log(`${opts.id} receiving newsnapshot from ${peer.id}`, s)
        journalup.snapshot(s.id, s.snapshot.snapshot, s.snapshot.to)
      })
    },
    toJSON: () => Object.keys(directory).map((key) => {
      const self = directory[key].self || {}
      const peers = directory[key].peers
      return {
        id: key,
        from: self.from,
        to: self.to,
        snapshotseq: self.snapshotseq,
        peers: Object.keys(peers).map(peer => {
          return {
            id: peer,
            from: peers[peer].from,
            to: peers[peer].to
          }
        }),
        subscribedto: directory[key].subscribedto
      }
    })
  }
}
