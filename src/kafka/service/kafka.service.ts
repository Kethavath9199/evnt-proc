import {
    Admin,
    Consumer,
    Kafka,
    Producer,
    ProducerRecord,
    RecordMetadata,
    SeekEntry
} from 'kafkajs';
import { SUBSCRIBER_OBJECT_MAP } from '../common/kafka.decorator';
import { KafkaMessageSend, KafkaModuleOption } from '../common/interfaces';
import { KafkaResponseDeserializer } from '../common/deserializer/kafka-response.deserializer';
import { SUBSCRIBER_MAP } from '../consumer/consumer.service';


export class KafkaService {
    private kafka: Kafka;
    private consumer: Consumer;
    private producer: Producer;
    private admin: Admin;
    private deserializer: any;
    private options: KafkaModuleOption['options'];

    protected topicOffsets: Map<
        string,
        (SeekEntry & { high: string; low: string })[]
    > = new Map();

    constructor(options: KafkaModuleOption['options']) {
        const { client, consumer: consumerConfig, producer: producerConfig } = options;

        this.kafka = new Kafka({
            ...client
        });

        console.log(`config: ${JSON.stringify(options)}`)

        const { groupId } = consumerConfig;
        const consumerOptions = Object.assign(
            {
                groupId: this.getGroupIdSuffix(groupId),
            },
            consumerConfig,
        );

        console.log(`consumerOptions: ${JSON.stringify(consumerOptions)}`)
        this.consumer = this.kafka.consumer(consumerOptions);
        this.producer = this.kafka.producer(producerConfig);
        this.admin = this.kafka.admin();

        this.initializeDeserializer(options);
        this.options = options;
    }

    async onModuleInit(): Promise<void> {
        await this.connect();
        // await this.getTopicOffsets();
        // SUBSCRIBER_MAP.forEach((functionRef, topic) => {
        //     console.log('topic', topic)
        //     this.subscribe(topic).then(() => {
        //         console.log(`successfully connected to topic ${topic}`)
        //     });
        // });
        // this.bindAllTopicToConsumer();
    }

    /**
     * Connect the kafka service.
     */
    async connect(): Promise<void> {
        await this.producer.connect().then(() => console.log('producer connected'))
            .catch((err) => console.log(`err in prod: ${err}`));
        // await this.consumer.connect().then(() => console.log('consumer connected'));;
        await this.admin.connect().then(() => console.log('admin connected'));;
    }

    /**
     * Disconnects the kafka service.
     */
    async disconnect(): Promise<void> {
        await this.producer.disconnect();
        // await this.consumer.disconnect();
        await this.admin.disconnect();
    }

    /**
     * Gets the high, low and partitions of a topic.
     */
    private async getTopicOffsets(): Promise<void> {
        console.log(`getTopicOffsets SUBSCRIBER_MAP : ${Array.from(SUBSCRIBER_MAP)}`)
        const topics = SUBSCRIBER_MAP.keys();
        console.log(`topics:${JSON.stringify(topics)}`)
        for await (const topic of topics) {
            try {
                const topicOffsets = await this.admin.fetchTopicOffsets(topic);
                this.topicOffsets.set(topic, topicOffsets);
            } catch (e) {
                console.error('Error fetching topic offset: ', topic);
            }
        }
    }

    /**
     * Subscribes to the topics.
     *
     * @param topic
     */
    private async subscribe(topic: string): Promise<void> {
        await this.consumer.subscribe({
            topic,
            fromBeginning: this.options.consumeFromBeginning || false,
        });
    }


    /**
     * Send/produce a message to a topic.
     *
     * @param message
     */
    async send(message: KafkaMessageSend): Promise<RecordMetadata[]> {
        if (!this.producer) {
            console.error('There is no producer, unable to send message.');
            throw new Error('no producer');
        }

        const record: ProducerRecord = {
            topic: message.topic ? message.topic : '',
            messages: message.messages,
        };

        // @todo - rather than have a producerRecord,
        // most of this can be done when we create the controller.
        return await this.producer.send(record);
    }

    /**
     * Gets the groupId suffix for the consumer.
     *
     * @param groupId
     */
    public getGroupIdSuffix(groupId: string): string {
        return groupId + '-client';
    }


    /**
     * Calls the method you are subscribed to.
     *
     * @param topic
     *  The topic to subscribe to.
     * @param instance
     *  The class instance.
     */
    subscribeToResponseOf<T>(topic: string, instance: T): void {
        SUBSCRIBER_OBJECT_MAP.set(topic, instance);
        console.log(`SUBSCRIBER_OBJECT_MAP:`, Array.from(SUBSCRIBER_OBJECT_MAP))
        console.log(`SUBSCRIBER_MAP:`, Array.from(SUBSCRIBER_MAP))
    }

    /**
     * Sets up the deserializer to decode incoming messages.
     *
     * @param options
     */
    protected initializeDeserializer(
        options: KafkaModuleOption['options'],
    ): void {
        this.deserializer =
            (options && options.deserializer) || new KafkaResponseDeserializer();
    }

    /**
     * Runs the consumer and calls the consumers when a message arrives.
     */
    private bindAllTopicToConsumer(): void {
        this.consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                console.log(`bindAllTopicToConsumer topic: ${topic}`)
                const objectRef = SUBSCRIBER_OBJECT_MAP.get(topic);
                console.log(`objectRef: ${objectRef}`)
                const callback = SUBSCRIBER_MAP.get(topic);
                console.log(`callback: ${callback}`)

                try {
                    const { timestamp, response, offset, key, headers } =
                        await this.deserializer.deserialize(message, { topic });
                    await callback.apply(objectRef, [
                        response,
                        headers,
                        key,
                        timestamp,
                        offset,
                        partition,
                    ]);
                } catch (e) {
                    console.error(`Error for message ${topic}: ${e}`);

                    // Log and throw to ensure we don't keep processing the messages when there is an error.
                    return;
                }
            },
        });

        if (this.options.seek !== undefined) {
            this.seekTopics();
        }
    }


    /**
    * Seeks to a specific offset defined in the config
    * or to the lowest value and across all partitions.
    */
    private seekTopics(): void {
        if (this.options.seek) {
            Object.keys(this.options.seek)?.forEach((topic) => {
                const topicOffsets = this.topicOffsets.get(topic);
                const seekPoint = this.options.seek?.[topic];

                topicOffsets?.forEach((topicOffset) => {
                    const seek =
                        seekPoint === 'earliest' ? topicOffset.low : String(seekPoint);

                    this.consumer.seek({
                        topic,
                        partition: topicOffset.partition,
                        offset: seek,
                    });
                });
            });
        }
    }

}

