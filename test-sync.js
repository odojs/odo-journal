const fs = require('fs')
const level = require('level-mem')
const sync = require('./sync')
const discovery = require('./swarm')
const logup = require('./log')
const journalup = require('./journal')
const sub = require('subleveldown')


const db1 = level()
const logs1 = logup(db1, { prefix: 'log.' })
const snapshots1 = sub(db1, 'snapshot.', {
  keyEncoding: 'utf8',
  valueEncoding: 'utf8'
})
const journals1 = journalup({ logs: logs1, snapshots: snapshots1 })
journals1.append('server1', [
  { e: 'test event', p: { id: 1, name: 'bob' } },
  { e: 'test event', p: { id: 2, name: 'nancy' } },
  { e: 'test event', p: { id: 3, name: 'drew' } },
  { e: 'test event', p: { id: 4, name: 'show' } },
  { e: 'test event', p: { id: 1, name: 'john' } }
], () => {
  const sync1 = sync({ id: 'server1', journals: journals1 })
  const server1 = discovery({
    id: 'server1',
    port: 19898,
    channel: 'testsync',
    key: fs.readFileSync('./key.pem'),
    cert: fs.readFileSync('./cert.pem'),
    ca: [ fs.readFileSync('./cert.pem') ]
  })
  server1.on('swarm.connection', (peer) => { sync1.add(peer) })
  setInterval(() => {
    console.log('server1 has', Object.values(sync1.toJSON()).map((v) => v.peers))
  }, 2000)
})


const db2 = level()
const logs2 = logup(db2, { prefix: 'log.' })
const snapshots2 = sub(db2, 'snapshot.', {
  keyEncoding: 'utf8',
  valueEncoding: 'utf8'
})
const journals2 = journalup({ logs: logs2, snapshots: snapshots2 })
journals2.append('server2', [
  { e: 'test event', p: { id: 1, name: 'bob' }},
  { e: 'test event', p: { id: 2, name: 'nancy' }}
], () => {
  const sync2 = sync({ id: 'server2', journals: journals2 })
  const server2 = discovery({
    id: 'server2',
    port: 19899,
    channel: 'testsync',
    key: fs.readFileSync('./key.pem'),
    cert: fs.readFileSync('./cert.pem'),
    ca: [ fs.readFileSync('./cert.pem') ]
  })
  server2.on('swarm.connection', (peer) => { sync2.add(peer) })
  setInterval(() => {
    journals2.append('server2', [
      { e: 'test event', p: { id: 3, name: 'john' }}
    ])
  }, 2000)
  // setInterval(() => {
  //   console.log('server2 has', sync2.toJSON())
  // }, 2000)
})
