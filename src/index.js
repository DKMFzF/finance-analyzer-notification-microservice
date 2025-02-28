const { Kafka } = require('kafkajs');
const nodemailer = require('nodemailer');

require('dotenv').config();

// Настройка Kafka
const kafka = new Kafka({
  clientId: 'notification-service',
  brokers: [process.env.KAFKA_BROKERS || 'kafka:9092'],
});

const consumer = kafka.consumer({ groupId: 'notification-group' });

// Настройка транспорта для отправки email (используем Gmail)
const transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: process.env.EMAIL_USER,
    pass: process.env.EMAIL_PASS,
  },
});

// Функция отправки письма
async function sendWelcomeEmail(toEmail) {
  try {
    await transporter.sendMail({
      from: `"AI Financial Analyzer" <${process.env.EMAIL_USER}>`,
      to: toEmail,
      subject: 'Добро пожаловать!',
      text: `Привет! Спасибо что используешь наше сервис для анализа финансов!`,
    });
    console.log(`[LOG]: Email sent to ${toEmail}`);
  } catch (error) {
    console.error('[ERROR]: Failed to send email:', error);
  }
}

async function startNotificationService() {
  try {
    await consumer.connect();
    await consumer.subscribe({ topic: 'user-registered', fromBeginning: true });

    console.log('[LOG]: Notification Service waiting for events...');

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const user = JSON.parse(message.value.toString());
        console.log(`[LOG]: Sending welcome email to ${user.email}`);

        console.log(user)

        await sendWelcomeEmail(user.login);
      },
    });
  } catch (error) {
    console.error('[ERROR]: Notification Service failed:', error);
  }
}

startNotificationService();
