import { Queue, Worker } from 'bullmq';

export const redisConfig = {
    host: 'localhost', // Replace with your Redis server host
    port: 6379, // Replace with your Redis server port
};

// Create a new connection in every instance
export const eventQueue = new Queue('cargoEvents',
    {
        connection: {
            host: redisConfig.host,
            port: redisConfig.port
        }
    }
);

export const emailNotificationQueue = new Queue('emailNotifications',
    {
        connection: {
            host: redisConfig.host,
            port: redisConfig.port
        }
    }
);

export async function clearEventQueue() {
    await eventQueue.drain();
    console.log(`cleared queue`)
}

