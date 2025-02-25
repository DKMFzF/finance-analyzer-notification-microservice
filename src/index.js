const { Kafka } = require('kafkajs');

// Настройка Kafka
const kafka = new Kafka({
  clientId: 'notification-service',
  brokers: [process.env.KAFKA_BROKERS || 'kafka:9092'],
});

const consumer = kafka.consumer({ groupId: 'notification-group' });

async function startNotificationService() {
  try {
    await consumer.connect();
    await consumer.subscribe({ topic: 'user-registered', fromBeginning: true });

    console.log('[LOG]: Notification Service waiting for events...');

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const user = JSON.parse(message.value.toString());
        console.log(`[LOG]: Sending welcome email to ${user.login}`);
        // Здесь можно добавить логику отправки email
      },
    });
  } catch (error) {
    console.error('[ERROR]: Notification Service failed:', error);
  }
}

startNotificationService();
