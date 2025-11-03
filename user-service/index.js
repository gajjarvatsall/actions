const express = require('express');
const amqp = require('amqplib');
const bodyParser = require('body-parser');

const PORT = 3001;
const RABBIT_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672';

const app = express();
app.use(bodyParser.json());

const users = [
  { id: 'u1', name: 'Alice' },
  { id: 'u2', name: 'Bob' }
];

app.get('/users', (req, res) => {
  res.json(users);
});

// optional endpoint to show processed events
let processed = [];
app.get('/processed', (req, res) => res.json(processed));

async function initRabbitConsumer() {
  try {
    const conn = await amqp.connect(RABBIT_URL);
    const ch = await conn.createChannel();
    await ch.assertExchange('events', 'topic', { durable: true });

    // durable queue for user-service, bind to post.created key
    const q = await ch.assertQueue('user-service-queue', { durable: true });
    await ch.bindQueue(q.queue, 'events', 'post.created');

    console.log('User-service waiting for post.created events...');
    ch.consume(q.queue, (msg) => {
      if (!msg) return;
      try {
        const content = JSON.parse(msg.content.toString());
        console.log('User-service consumed event post.created ->', content.id);
        // simulate processing
        processed.push({ event: 'post.created', postId: content.id, at: new Date().toISOString() });
        ch.ack(msg);
      } catch (err) {
        console.error('Processing error', err);
        ch.nack(msg, false, false); // discard or dead-letter per your policy
      }
    }, { noAck: false });
  } catch (err) {
    console.error('Rabbit consumer error', err);
    process.exit(1);
  }
}

app.listen(PORT, '0.0.0.0', async () => {
  console.log(`User service listening on ${PORT}`);
  await initRabbitConsumer();
});
