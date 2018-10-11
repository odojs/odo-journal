module.exports = (store, source, target) => {
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
      else if (s[key] !== t[key]) d[key] = t[key]
    ops.push({ type: 'put', store: store, id: id, value: d })
  }
  for (let id of Object.keys(target))
    if (!source[id])
      ops.push({ type: 'put', store: store, id: id, value: target[id] })
  return ops
}
