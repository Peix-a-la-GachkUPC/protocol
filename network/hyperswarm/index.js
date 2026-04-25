const Hyperswarm = require('hyperswarm')
const crypto = require('crypto')

const messages = []
let waitingForRecv = false
let swarm = null
let conns = new Set()

function writeResponse(data) {
  process.stdout.write(JSON.stringify(data) + '\n')
}

function writeEvent(data) {
  process.stderr.write(JSON.stringify(data) + '\n')
}

function processCommand(line) {
  let cmd
  try {
    cmd = JSON.parse(line)
  } catch (e) {
    writeResponse({ error: 'Invalid JSON', raw: line })
    return
  }

  switch (cmd.cmd) {
    case 'create':
      if (!cmd.topic) {
        writeResponse({ status: 'error', error: 'Missing topic', cmd: 'create' })
        return
      }
      doCreate(cmd.topic)
      break

    case 'send':
      if (!cmd.msg) {
        writeResponse({ status: 'error', error: 'Missing msg', cmd: 'send' })
        return
      }
      doSend(cmd.msg)
      break

    case 'recv':
      doRecv()
      break

    case 'nrecv':
      doNrecv()
      break

    case 'peers':
      doPeers()
      break

    default:
      writeResponse({ error: ' Unknown command', cmd: cmd.cmd })
  }
}

function doCreate(topic) {
  if (swarm) {
    swarm.destroy()
  }

  messages.length = 0
  waitingForRecv = false
  conns.clear()

  const topicBuffer = crypto.createHash('sha256').update(topic).digest()

  swarm = new Hyperswarm()

  swarm.on('connection', (conn, info) => {
    const peerId = conn.remotePublicKey ? conn.remotePublicKey.toString('hex').slice(0, 8) : 'unknown'
    conns.add(conn)

    writeEvent({ event: 'peer_connected', id: peerId })

    conn.on('data', (data) => {
      const msg = data.toString().trim()
      if (msg) {
        messages.push(msg)
        if (waitingForRecv) {
          waitingForRecv = false
          const msgToSend = messages.shift()
          writeResponse({ msg: msgToSend, cmd: 'recv' })
        }
      }
    })

    conn.on('close', () => {
      conns.delete(conn)
      writeResponse({ event: 'peer_disconnected', id: peerId })
    })

    conn.on('error', (err) => {
      conns.delete(conn)
    })
  })

  const discovery = swarm.join(topicBuffer, { client: true, server: true, limit: 64 })

  discovery.flushed().then(() => {
    writeResponse({ status: 'ok', cmd: 'create' })
  }).catch(err => {
    writeResponse({ status: 'error', error: err.message, cmd: 'create' })
  })
}

function doSend(msg) {
  if (!swarm) {
    writeResponse({ status: 'error', error: 'Not connected', cmd: 'send' })
    return
  }

  for (const conn of conns) {
    try {
      conn.write(msg + '\n')
    } catch (e) {
    }
  }

  writeResponse({ status: 'ok', cmd: 'send' })
}

function doRecv() {
  if (messages.length > 0) {
    const msg = messages.shift()
    writeResponse({ msg: msg, cmd: 'recv' })
    return
  }

  waitingForRecv = true
}

function doNrecv() {
  if (messages.length > 0) {
    const msg = messages.shift()
    writeResponse({ msg: msg, cmd: 'nrecv' })
  } else {
    writeResponse({ msg: null, cmd: 'nrecv' })
  }
}

function doPeers() {
  writeResponse({ count: conns.size, cmd: 'peers' })
}

process.stdin.setEncoding('utf8')

let buffer = ''

process.stdin.on('data', (chunk) => {
  buffer += chunk

  let newlineIndex
  while ((newlineIndex = buffer.indexOf('\n')) !== -1) {
    const line = buffer.slice(0, newlineIndex).trim()
    buffer = buffer.slice(newlineIndex + 1)

    if (line) {
      processCommand(line)
    }
  }
})

process.on('exit', () => {
  if (swarm) {
    swarm.destroy()
  }
})

process.on('SIGINT', () => {
  if (swarm) {
    swarm.destroy()
  }
  process.exit(0)
})

process.on('SIGTERM', () => {
  if (swarm) {
    swarm.destroy()
  }
  process.exit(0)
})