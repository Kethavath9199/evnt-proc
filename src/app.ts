import express from 'express';
import * as dotenv from 'dotenv'
import { connect, set } from 'mongoose';
import errorMiddleware from './middlewares/errorHandling.middleware';
import { Routes } from 'interfaces/route.interface';
import { NODE_ENV } from './config/env.config';
import { Logger } from './utils/logger';
import { DI } from './di/di.container';
import { MongoConnection } from './connections/database/mongo.connection';
import { EventsService } from './services/events.service';
import { EventQueueProducer } from './queue/queueProducer/eventQueue.producer';
import { CronJob } from 'cron';
import { EventQueueProcessor } from './queue/queueProcessor/eventQueue.processor';
import { EmailNotificationQueueProcessor } from './queue/queueProcessor/emailNotificationQueue.processor';
import { clearEventQueue } from './queue/queueConfig/eventQueue.config';
import { ProducerService } from './kafka/producer/producer.service';

dotenv.config();

class App {
    public app = express();
    public env: string;
    public port = process.env.PORT || 3000;
    private logger: Logger;
    private mongoDb: MongoConnection;
    // private eventService: EventsService;
    private eventQueueProducer: EventQueueProducer;
    private eventQueueProcessor: EventQueueProcessor
    private emailNotificationQueueProcessor: EmailNotificationQueueProcessor;
    private producerService: ProducerService

    constructor(routes: Routes[]) {
        this.logger = DI.get<Logger>(Logger)
        this.mongoDb = DI.get<MongoConnection>(MongoConnection)
        // this.eventService = DI.get<EventsService>(EventsService)
        this.eventQueueProducer = DI.get<EventQueueProducer>(EventQueueProducer)
        this.eventQueueProcessor = DI.get<EventQueueProcessor>(EventQueueProcessor)  //starting event consumer
        this.emailNotificationQueueProcessor = DI.get<EmailNotificationQueueProcessor>(EmailNotificationQueueProcessor)  //starting email consumer
        this.producerService = DI.get<ProducerService>(ProducerService)

        this.app = express();
        this.env = NODE_ENV || 'development';
        this.port = this.port || 3000;

        this.databaseConnection();
        this.initializeMiddlewares();
        this.initializeRoutes(routes);
        this.initializeErrorHandling();
    }

    public listen() {
        this.app.listen(this.port, async () => {

            await this.producerService.onModuleInit();

            //console.log('keycloak', keycloak);
            this.logger.info(`=================================`);
            this.logger.info(`======= ENV: ${this.env} =======`);
            this.logger.info(`🚀 App listening on the port ${this.port}`);
            this.logger.info(`=================================`);
            this.logger.info(`DB Details${process.env.DB_HOST} `);
            // clearEventQueue()

            //----------------------Schedulers-----------------------------------//
            var cronService = new CronJob("*/15 * * * *", async () => {
                const currentDate = new Date();
                this.logger.debug("current date and time : ", currentDate);
                // await this.eventService.insertEvents();
                await this.eventQueueProducer.addJobToEventQueue({ date: new Date() })
                this.logger.debug('cron Execution Success');
            });
            cronService.start();

        });
    }


    private initializeMiddlewares() {
        this.app.use(express.json());
        this.app.use(express.urlencoded({ extended: true }));
    }

    private initializeRoutes(routes: Routes[]) {
        routes.forEach(route => {
            this.app.use('/', route.router);
        });
    }

    private initializeErrorHandling() {
        this.app.use(errorMiddleware);
    }

    private databaseConnection() {
        this.mongoDb.connect().then((res) => {
            console.log('mongo connected succesfully.....')
        })
    }
}

export default App;
