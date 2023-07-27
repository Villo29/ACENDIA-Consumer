import amqp from 'amqplib';
import fetch from 'node-fetch';

const rabbitSettings = {
  protocol: 'amqp',
  hostname: '44.206.123.12',
  port: 5672,
  username: 'guest',
  password: 'guest',
};

async function consumeMessages() {
  try {
    const conn = await amqp.connect(rabbitSettings);
    console.log('Conexión exitosa');

    const channel = await conn.createChannel();
    console.log('Canal creado');

    const queue = 'Datos';

    await channel.assertQueue(queue);
    console.log('Cola creada');

    channel.consume(queue, async (message) => {
      if (message !== null) {
        const messageContent = message.content.toString();
        console.log('Mensaje recibido:', messageContent);
        console.log("Prueba", messageContent)
        try {
          const apiUrl = 'http://localhost:3000/video';
          const options = {
            method: 'POST',
            body: JSON.stringify({ Personas: messageContent}),
            headers: {
              'Content-Type': 'application/json'
            }
          };
          const response = await fetch(apiUrl, options);
          const contentType = response.headers.get('content-type');
          if (contentType.includes('application/json')) {
            const data = await response.json();
            console.log('Respuesta de la API:', data);
          } else {
            const text = await response.text();
            console.error('Error en la respuesta de la API:', text);
          }
          // Eliminar el mensaje de la cola una vez procesado
          channel.ack(message);
          console.log('Mensaje eliminado de la cola');
        } catch (error) {
          console.error('Error al hacer la solicitud a la API:', error);
          // Rechazar (rechazar) el mensaje sin volver a encolarlo
          channel.reject(message, false);
          console.log('Error al procesar el mensaje, se elimina de la cola');
        }
      }
    });
  } catch (error) {
    console.error('Error:', error);
  }
}

consumeMessages();