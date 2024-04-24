import { Worker, Job } from 'bullmq';
import { Logger } from '../../utils/logger';
import { DI } from '../../di/di.container';
// import { eventQueue } from '../queueConfig/eventQueue.config';
import { EventsService } from '../../services/events.service';

export class EventQueueProcessor {
    private eventQueueProcessor: Worker;
    private logger: Logger;
    private eventsService: EventsService;

    constructor() {
        this.logger = DI.get<Logger>(Logger);
        this.eventsService = DI.get<EventsService>(EventsService);

        this.logger.debug(`eventQueue Processor Started`);
        this.eventQueueProcessor = new Worker('cargoEvents', async (job: Job) => {
            try {
                this.logger.debug(`Event Queue processor job started jobId: ${job.id} with data: ${JSON.stringify(job.data.data)} at timeStamp:${new Date()},`);
                let result: any = await this.eventsService.persistanceOfEvents();
                this.logger.debug(`result after inserting events ${result}`)
            } catch (err) {
                this.logger.error(`Error in executing the job: ${job.id} with Error: ${err}`);
                // if (job.attemptsMade <= 3) {
                //     job.retry();
                //     this.logger.debug(`Retrying job ${job.id} (attempt ${job.attemptsMade + 1})`);
                // } else {
                //     this.logger.debug(`Job ${job.id} has reached the maximum retry attempts.`);
                // }
            }

        });

        this.eventQueueProcessor.on('completed', (job: Job, returnValue: any) => {
            this.logger.debug(`Event Queue processor Completing job ${job.id} with data: ${job.data.data} with returnValue: ${returnValue}`);
        });

        this.eventQueueProcessor.on('progress', (job: Job, progress: number | object) => {
            this.logger.debug(`Event Queue processor Processing job ${job.id} with data: ${job.data.data} with progress data: ${progress}`);
        });

        this.eventQueueProcessor.on('failed', (job: Job, error: Error) => {
            this.logger.debug(`Event Queue processor Failed job ${job.id} with data: ${JSON.stringify(job.data.data)} with error: ${error.message}`);
        });
    }
}

