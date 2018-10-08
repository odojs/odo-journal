const level = require('level-mem')
const esup = require('./es')

const db = level()
const es = esup(db)

es.batch([
  { type: 'put', store: 'people', id: 1, value: { name: 'nancy' } },
  { type: 'put', store: 'people', id: 1, value: { occupation: 'teacher' } },
  { type: 'put', store: 'people', id: 2, value: { name: 'bob' } },
  { type: 'del', store: 'people', id: 2 },
  { type: 'put', store: 'people', id: 3, value: { name: 'bob' } },
], (err) => {
  es.all('people', (err, people) => {
    console.log(people)
  })
  es.get('people', 1, (err, person) => {
    console.log(person)
  })
})
