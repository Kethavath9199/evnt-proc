import { RecordMetadata } from 'kafkajs';
import { v4 as uuidv4 } from 'uuid';
import { KafkaService } from '../service/kafka.service';
import { DI } from '../../di/di.container';
import { KafkaModuleOption } from '../common/interfaces';
import dotenv from 'dotenv'
import { Logger } from '../../utils/logger';
dotenv.config()

export class ProducerService {

    private logger: Logger
    private client: KafkaService;
    constructor() {
        const options: KafkaModuleOption['options'] = {
            client: {
                clientId: 'linesight-backend',
                brokers: process.env.KAFKA_BROKERS?.split(',') ?? []
            },
            consumer: {
                groupId: process.env.KAFKA_GROUP_ID ?? '',
            }
        };

        this.client = DI.get<KafkaService>(KafkaService, options);
        this.logger = DI.get<Logger>(Logger)
    }

    async onModuleInit(): Promise<any> {
        await this.client.onModuleInit()
    }


    async post(topic: string, message: string): Promise<RecordMetadata[]> {
        this.logger.debug(`timestamp: ${new Date()} - topic: ${topic} - msg : ${JSON.stringify(message)}`)
        return this.client.send({
            topic: topic,
            messages: [
                {
                    key: `${uuidv4()}`,
                    value: JSON.stringify(message),
                },
            ],
        });
    }

    async postToPartition(topic: string, partition: number, message: string): Promise<RecordMetadata[]> {
        this.logger.debug(`timestamp: ${new Date()} - topic: ${topic} - msg : ${JSON.stringify(message)}`)
        return this.client.send({
            topic: topic,
            messages: [
                {
                    key: `${uuidv4()}`,
                    value: JSON.stringify(message),
                },
            ],
            partition: partition,
        });
    }

}