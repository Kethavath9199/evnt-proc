import { Logger } from '../../utils/logger';
import { DI } from '../../di/di.container';
import { eventQueue } from '../queueConfig/eventQueue.config'
import { emailNotificationQueue } from '../queueConfig/eventQueue.config';

export class EventQueueProducer {
  private logger: Logger;

  constructor() {
    this.logger = DI.get<Logger>(Logger);
  }

  async addJobToEventQueue(data: any) {
    this.logger.debug(`event queue data:${data}, timeStamp:${new Date()}`)
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

    await eventQueue.add(
      'cargoEvents',
      jobData,  
      jobOptions,
    );

    this.logger.debug(`job added to eventQueue with data:${jobData}`)
  }

  async addJobToEmailNotificationQueue(data: any) {
    this.logger.debug(`email notification queue data:${data}, timeStamp:${new Date()}`)
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
  }
}