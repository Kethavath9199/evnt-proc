import { Worker, Job } from 'bullmq';
import { Logger } from '../../utils/logger';
import { DI } from '../../di/di.container';
// import { eventQueue } from '../queueConfig/eventQueue.config';
import { EmailService } from '../../services/emailServices';

export class EmailNotificationQueueProcessor {
    private emailNotificationQueueProcessor: Worker;
    private logger: Logger;
    private emailService: EmailService;

    constructor() {
        this.logger = DI.get<Logger>(Logger);
        this.emailService = DI.get<EmailService>(EmailService);

        this.logger.debug(`email Notification Queue Processor Started`);
        this.emailNotificationQueueProcessor = new Worker('emailNotifications', async (job: Job) => {
            try {
                this.logger.debug(`Email Notification Queue Processor  job started jobId: ${job.id} with data: ${JSON.stringify(job.data.data)} at timeStamp:${new Date()},`);
                let data: any = job.data.data;
                let result: any = await this.emailService.sendMailNotification(data);
                this.logger.debug(`result after inserting events ${result}`)
            } catch (err) {
                this.logger.error(`Error in executing the Email Notification Queue job: ${job.id} with Error: ${err}`);
                // if (job.attemptsMade <= 3) {
                //     job.retry();
                //     this.logger.debug(`Retrying Email Notification Queue job: ${job.id} (attempt ${job.attemptsMade + 1})`);
                // } else {
                //     this.logger.debug(`Email Notification Queue Job: ${job.id} has reached the maximum retry attempts.`);
                // }
            }

        });

        this.emailNotificationQueueProcessor.on('completed', (job: Job, returnValue: any) => {
            this.logger.debug(`Email Notification Queue Processor Completing job: ${job.id} with data: ${job.data.data} with returnValue: ${returnValue}`);
        });

        this.emailNotificationQueueProcessor.on('progress', (job: Job, progress: number | object) => {
            this.logger.debug(`Email Notification Queue Processor Processing job: ${job.id} with data: ${job.data.data} with progress data: ${progress}`);
        });

        this.emailNotificationQueueProcessor.on('failed', (job: Job, error: Error) => {
            this.logger.debug(`Email Notification Queue Processor Failed job: ${job.id} with data: ${JSON.stringify(job.data.data)} with error: ${error.message}`);
        });
    }
}

