const level = require('level-mem')
const esup = require('./es')

const db = level()
const es = esup(db)

const diff = require('./esdiff')

es.batch([
  { type: 'put', store: 'people', id: 1, value: { name: 'nancy' } },
  { type: 'put', store: 'people', id: 1, value: { occupation: 'teacher' } },
  { type: 'put', store: 'people', id: 2, value: { name: 'bob' } },
  { type: 'del', store: 'people', id: 2 },
  { type: 'put', store: 'people', id: 3, value: { name: 'bob' } },
])
.then(() => {
  es.all('people')
    .then((people) => {
      console.log(diff('people', people, {
        '1': { name: 'judy', occupation: 'teacher' }
      }))
    })
    .catch(console.error)
  // es.get('people', 1)
  //   .then((person) => console.log(person))
  //   .catch(console.error)
})
