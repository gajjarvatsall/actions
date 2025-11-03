const express = require('express');
const bodyParser = require('body-parser');
const amqp = require('amqplib');
const { v4: uuidv4 } = require('uuid');

const PORT = 3002;
const RABBIT_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672';
let channel = null;

const app = express();
app.use(bodyParser.json());

// In-memory posts store (for demo)
const posts = [];

// Connect to RabbitMQ and create exchange
async function initRabbit() {
  try {
    const conn = await amqp.connect(RABBIT_URL);
    channel = await conn.createChannel();
    await channel.assertExchange('events', 'topic', { durable: true });
    console.log('Post-service connected to RabbitMQ');
  } catch (err) {
    console.error('RabbitMQ connect error', err);
    process.exit(1);
  }
}

// Create a post (sync HTTP) and publish event (async)
app.post('/posts', async (req, res) => {
  const { title, body } = req.body || {};
  if (!title) return res.status(400).json({ error: 'title required' });

  const post = { id: uuidv4(), title, body: body || '', createdAt: new Date().toISOString() };
  posts.push(post);

  // publish async event
  try {
    const routingKey = 'post.created';
    const payload = Buffer.from(JSON.stringify(post));
    channel.publish('events', routingKey, payload, { persistent: true });
    console.log('Published event post.created', post.id);
  } catch (err) {
    console.error('Failed to publish event', err);
  }

  res.status(201).json(post);
});

// GET posts
app.get('/posts', (req, res) => {
  res.json(posts);
});

app.listen(PORT, '0.0.0.0', async () => {
  console.log(`Post service listening on ${PORT}`);
  await initRabbit();
});
