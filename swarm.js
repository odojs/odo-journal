const dc = require('discovery-channel')
const tls = require('tls')
const EventEmitter = require('events')

// const server1 = discovery({
//   id: 'server1',
//   port: 19898,
//   channel: 'test channel',
//   key: fs.readFileSync('../tls/key.pem'),
//   cert: fs.readFileSync('../tls/cert.pem'),
//   ca: [ fs.readFileSync('../tls/cert.pem') ]
// })
// server1.on('swarm.connection', (peer) => {
//   console.log(`server1 -> ${peer.id}`)
//   peer.on('swarm.disconnect', (reason) => {
//     console.log(`server1 XX ${peer.id}`)
//   })
// })

module.exports = (options, cb) => {
  const result = new EventEmitter()
  const peersbykey = {}
  const activepeers = {}
  const peerhistory = {}

  // defaults
  const pinginterval = options.pinginterval != null
    ? options.pinginterval
    : 30 * 1000 // 30s
  const purgetimeout = options.purgetimeout != null
    ? options.purgetimeout
    : 60 * 10 * 1000 // 1h
  const tickinterval = options.tickinterval != null
    ? options.tickinterval
    : 5000 // 5s
  const backofftimings = options.backofftimings != null
    ? options.backofftimings
    : [
      5 * 1000, // 5s
      10 * 1000, // 10s
      30 * 1000, // 30s
      1 * 60 * 1000, // 1m
      10 * 60 * 1000, // 10m
      60 * 60 * 1000 // 1h
    ]
  const serverhost = options.host != null ? options.host : 'localhost'

  const sendtolifesupport = (peer, reason) => {
    //console.log(`${options.id} ${peer.id || peer.key} lifesupport ${reason}`)
    peer.activity = Date.now()
    peer.state = 'lifesupport'
    peer.reason = reason
    if (peer.socket != null) {
      peer.socket.end()
      peer.socket.destroy()
      peer.socket = null
    }
    if (activepeers[peer.id] != null) {
      delete activepeers[peer.id]
      peerhistory[peer.id].activity = peer.activity
      peer.emit('swarm.disconnect', reason)
    }
    peer.removeAllListeners()
  }

  const deletepeer = (peer, reason) => {
    //console.log(`${options.id} ${peer.id || peer.key} deleted ${reason}`)
    peer.state = 'deleted'
    peer.reason = reason
    if (peersbykey[peer.key] != null) delete peersbykey[peer.key]
    if (peer.socket != null) {
      peer.socket.end()
      peer.socket.destroy()
      peer.socket = null
    }
    if (peer.id != null && activepeers[peer.id] != null) {
      delete activepeers[peer.id]
      peerhistory[peer.id].activity = peer.activity
      peer.emit('swarm.disconnect', reason)
    }
    peer.removeAllListeners()
  }

  const connect = (peer) => {
    const clientoptions = {
      key: options.key,
      cert: options.cert,
      ca: options.ca,
      rejectUnauthorized: true,
      requestCert: true,
      host: peer.host,
      port: peer.port,
      checkServerIdentity: (host, cert) => undefined
    }

    const socket = tls.connect(clientoptions, () => { peer.activity = Date.now() })
    socket.setEncoding('utf8')
    let backlog = ''
    socket.on('data', (data) => {
      backlog += data
      let index = backlog.indexOf('\n')
      while (index != -1) {
        const payload = JSON.parse(backlog.substring(0, index))
        backlog = backlog.substring(index + 1)
        index = backlog.indexOf('\n')
        peer.activity = Date.now()
        //console.log(`client ${peer.id || peer.key} -> ${options.id} received`, payload)
        peer.emit('event', payload)
        peer.emit(payload.e, payload.p)
      }
    })
    socket.on('close', () => {
      sendtolifesupport(peer, 'outgoing connection closed')
    })
    socket.on('error', (err) => { sendtolifesupport(peer, 'outgoing client error') })
    peer.socket = socket
    peer.on('swarm.howareyou', (data) => {
      if (data.id == options.id)
        return sendtolifesupport(peer, 'outgoing connection detected to self')
      if (activepeers[data.id] != null) {
        peer.write('swarm.iamalreadyconnected')
        return sendtolifesupport(peer, 'duplicate outgoing connection')
      }
      peer.id = data.id
      peerhistory[data.id] = {
        id: peer.id,
        host: peer.host,
        port: peer.port,
        activity: peer.activity
      }
      peer.state = 'active'
      peer.attempt = 1
      activepeers[peer.id] = peer
      peersbykey[peer.key] = peer
      result.emit('swarm.connection', peer)
      peer.write('swarm.iamfine', { id: options.id })
    })
    peer.on('swarm.iamfine', (data) => {
      peer.emit('swarm.ready')
    })
    peer.on('swarm.ping', (data) => { peer.write('swarm.pong') })
    peer.on('swarm.pong', (data) => { })
    peer.on('swarm.iamalreadyconnected', () => {
      sendtolifesupport(peer, 'outgoing connection says already connected')
    })
  }

  const tick = () => {
    const currenttime = Date.now()
    for (let peer of Object.values(peersbykey)) {
      const delta = currenttime - peer.activity
      switch (peer.state) {
        case 'active':
          if (delta < pinginterval) break
          peer.write('swarm.ping')
          break
        case 'lifesupport':
          if (!peer.zombie && backofftimings[peer.attempt] == null) {
            deletepeer(peer, 'off life support')
            break
          }
          const delay = backofftimings[peer.attempt]
            || backofftimings[backofftimings.length - 1]
          if (delta < delay) break
          peer.state = 'outgoing'
          peer.attempt++
          connect(peer)
          break
      }
    }
  }
  setInterval(tick, tickinterval)

  result.add = (host, port, zombie) => {
    const peer = new EventEmitter()
    peer.key = `${host}:${port}`
    peer.id = null
    peer.host = host
    peer.port = port
    peer.zombie = zombie
    peer.state = 'outgoing'
    peer.attempt = 1
    peer.activity = Date.now()
    peer.reason = null
    peer.socket = null
    peer.write = (event, payload) => {
      if (peer.socket == null) {
        console.trace(`${options.id} error: client is writing to destroyed socket`)
        console.error('Event', event)
        console.error('Payload', payload)
        console.error('Peer', peer)
        return
      }
      //console.log(`client ${options.id} -> ${peer.id || peer.key} sent`, { e: event, p: payload })
      peer.socket.write(JSON.stringify({ e: event, p: payload }) + '\n')
    }
    if (peersbykey[peer.key] != null) return
    peersbykey[peer.key] = peer
    connect(peer)
  }

  const swam = dc()
  swam.on('peer', (topic, ref, type) => result.add(ref.host, ref.port))

  const serverOptions = {
    key: options.key,
    cert: options.cert,
    ca: options.ca,
    rejectUnauthorized: true,
    requestCert: true,
    host: serverhost,
    port: options.port,
    checkServerIdentity: (host, cert) => undefined
  }

  const server = tls.createServer(serverOptions, (socket) => {
    const ref = socket.address()
    const peer = new EventEmitter()
    peer.key = `${ref.address}:${ref.port}`
    peer.id = null
    peer.host = ref.address
    peer.port = ref.port
    peer.state = 'incoming'
    peer.attempt = 1
    peer.activity = Date.now()
    peer.socket = socket
    peer.reason = null
    peer.write = (event, payload) => {
      if (socket.destroyed) {
        console.trace(`${options.id} error: server is writing to destroyed socket`)
        console.error('Event', event)
        console.error('Payload', payload)
        console.error('Peer', peer)
        return
      }
      //console.log(`server ${options.id} -> ${peer.id || peer.key} sent`, { e: event, p: payload })
      socket.write(JSON.stringify({ e: event, p: payload }) + '\n')
    }
    peer.on('swarm.iamfine', (data) => {
      peer.id = data.id
      peerhistory[peer.id] = {
        id: peer.id,
        host: peer.host,
        port: peer.port,
        activity: peer.activity
      }
      if (activepeers[peer.id] != null) {
        peer.write('swarm.iamalreadyconnected')
        sendtolifesupport(peer, 'duplicate peer on incoming connection')
        return
      }
      peer.state = 'active'
      activepeers[peer.id] = peer
      peersbykey[peer.key] = peer
      result.emit('swarm.connection', peer)
      peer.emit('swarm.ready')
      peer.write('swarm.iamfine', { id: options.id })
    })
    peer.on('swarm.ping', (data) => { peer.write('swarm.pong') })
    peer.on('swarm.pong', (data) => { })
    peer.on('swarm.iamalreadyconnected', (data) => {
      sendtolifesupport(peer, 'incoming connection says already connected')
    })
    socket.on('error', (error) => { deletepeer(peer, 'server error') })
    socket.on('close', () => { deletepeer(peer, 'incoming connection closed') })
    //console.log(`server ${options.id} -> ${peer.key} sending how are you`)
    peer.write('swarm.howareyou', { id: options.id })
    socket.setEncoding('utf8')
    let backlog = ''
    socket.on('data', (data) => {
      backlog += data
      let index = backlog.indexOf('\n')
      while (index != -1) {
        const payload = JSON.parse(backlog.substring(0, index))
        backlog = backlog.substring(index + 1)
        index = backlog.indexOf('\n')
        peer.activity = Date.now()
        //console.log(`server ${peer.id || peer.key} -> ${options.id} received`, payload)
        peer.emit('event', payload)
        peer.emit(payload.e, payload.p)
      }
    })
  })
  server.on('error', (err) => { result.emit('error', err) })
  server.on('tlsClientError', (exception, socket) => { const ref = socket.address() })
  server.on('listening', () => {
    const host = server.address().address
    const boundport = server.address().port
    result.emit('open', { host: host, port: boundport })
  })
  server.listen(options.port, () => { swam.join(options.channel, options.port) })
  result.toJSON = () => { return Object.values(peersbykey).map((peer) => {
    return {
      id: peer.id,
      host: peer.host,
      port: peer.port,
      state: peer.state,
      activity: peer.activity,
      reason: peer.reason
    }
  })}
  result.history = () => peerhistory
  return result
}
