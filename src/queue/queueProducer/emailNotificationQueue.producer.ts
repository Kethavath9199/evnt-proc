import { Logger } from '../../utils/logger';
import { DI } from '../../di/di.container';
import { emailNotificationQueue } from '../queueConfig/eventQueue.config';

export class EmailNotificationQueueProducer {
    private logger: Logger;

    constructor() {
        this.logger = DI.get<Logger>(Logger);
    }

    async addJobToEmailNotificationQueue(data: any) {
        return new Promise(async (resolve, reject) => {
            try {
                this.logger.debug(`email notification queue data:${data} at timeStamp:${new Date()}`)
                const jobData = { data: data };
                const jobOptions = {
                    attempts: 3,   // Number of retry attempts
                    backoff: {     // Set the backoff options
                        type: 'exponential', // exponential
                        delay: 5000, // Retry delay in milliseconds (e.g., 5 seconds)
                    },
                    removeOnComplete: true,
                    removeOnFail: true
                };

                await emailNotificationQueue.add(
                    'emailNotifications',
                    jobData,
                    jobOptions,
                );

                this.logger.debug(`job added to topic: emailNotifications,  with data:${jobData} at timeStamp:${new Date()}`)
                return resolve(`job successfully added to topic: emailNotifications at timeStamp:${new Date()}`)
            } catch (err) {
                this.logger.error(`job added to topic: emailNotifications,  with data:${data} at timeStamp:${new Date()}`)
                return reject(`Job failed to add to topic: emailNotifications, with data: ${data} at timeStamp: ${new Date()}`);

            }
        })
    }
}