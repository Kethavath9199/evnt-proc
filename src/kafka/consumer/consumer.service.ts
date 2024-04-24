// import { SUBSCRIBER_MAP } from "../common/kafka.decorator";
import { KafkaService } from "../service/kafka.service";
import { DI } from '../../di/di.container';
import { Logger } from '../../utils/logger';
import { KafkaModuleOption } from "../common/interfaces";

export const SUBSCRIBER_MAP = new Map();
const bulkUploadTopic = process.env.KAFKA_TOPIC_BULK_UPLOADS || '';

export class ConsumerService {
    private client: KafkaService;
    private readonly logger: Logger;
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
        this.client = DI.get<KafkaService>(KafkaService, options)
        this.logger = DI.get<Logger>(Logger)

    }

    // async onModuleInit(): Promise<any> {
    //     if (!bulkUploadTopic) {
    //         throw new Error(
    //             'Make sure KAFKA_TOPIC_BULK_UPLOADS are set in the environment variables',
    //         );
    //     }
    //     await this.client.subscribeToResponseOf(bulkUploadTopic, this);
    //     await this.client.onModuleInit()
    // }
}
