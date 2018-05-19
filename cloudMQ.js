// This file will connect to a cloudAMQP connection,
// post a message to the exchange, and listen on a queue

// ampq dependancy
var amqp = require('amqplib/callback_api')
var ampqConn = null
const constant = require('./constant')

function start() {
  amqp.connect(constant.amqp_url + "?heartbeat=60", function(err, conn) {
    if (err) {
      console.error("[AMQP]", err.message)
      return setTimeout(start, 1000)
    }
    conn.on("error", function(err) {
      if (err.message !== "Connection closing") {
        console.error("[AMQP] conn error", err.message)
      }
    })
    conn.on("close", function() {
      console.error("[AMQP] reconnecting")
      return setTimeout(start, 1000)
    })
    console.log("[AMQP] connected")
    amqpConn = conn
    whenConnected()
  })
}

function whenConnected(){
  startPublisher()
  startListener()
}

// start publisher
var pubChannel = null
var offlinePubQueue = []
function startPublisher() {
  amqpConn.createConfirmChannel(function(err, ch) {
    if (closeOnErr(err)) return
      ch.on("error", function(err) {
      console.error("[AMQP] channel error", err.message)
    })
    ch.on("close", function() {
      console.log("[AMQP] channel closed")
    });

    pubChannel = ch
    while (true) {
      var m = offlinePubQueue.shift()
      if (!m) break
      publish(m[0], m[1], m[2])
    }
  });
}

// publish
function publish(exchange, routingKey, content) {
  try {
    pubChannel.publish(exchange, routingKey, content, { persistent: true },
                      function(err, ok) {
                        if (err) {
                          console.error("[AMQP] publish", err)
                          offlinePubQueue.push([exchange, routingKey, content])
                          pubChannel.connection.close()
                        }
                      })
  } catch (e) {
    console.error("[AMQP] publish", e.message)
    offlinePubQueue.push([exchange, routingKey, content])
  }
}

// Consume messages
// A worker that acks messages only if processed successfully
function startListener() {
  amqpConn.createChannel(function(err, ch) {
    if (closeOnErr(err)) return
    ch.on("error", function(err) {
      console.error("[AMQP] channel error", err.message)
    })
    ch.on("close", function() {
      console.log("[AMQP] channel closed")
    })

    ch.prefetch(10)
    ch.assertQueue("light", { durable: true }, function(err, _ok) {
      if (closeOnErr(err)) return;
      ch.consume("light", processMsg, { noAck: false })
      console.log("Worker is started")
    })

    function processMsg(msg) {
      work(msg, function(ok) {
        try {
          if (ok)
            ch.ack(msg)
          else
            ch.reject(msg, true)
        } catch (e) {
          closeOnErr(e)
        }
      })
    }
  })
}

function work(msg, cb) {
  console.log("Message content: ", msg.content.toString())
  cb(true)
}

// close the connection on error
function closeOnErr(err) {
  if (!err) return false
  console.error("[AMQP] error", err)
  amqpConn.close()
  return true
}

// setInterval(function() {
//   publish("", "light", new Buffer("LED off"))
// }, 2000)

// setInterval(() => {
//   publish("", "light", new Buffer('LED on'))
// }, 1000)

start()