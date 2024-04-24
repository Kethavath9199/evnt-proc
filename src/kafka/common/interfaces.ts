import {
    ConsumerConfig,
    KafkaConfig,
    ProducerConfig,
    ProducerRecord,
    Message,
    IHeaders,
} from 'kafkajs';



export interface KafkaResponse<T = any> {
    response: T;
    key: string;
    timestamp: string;
    offset: string;
    headers?: IHeaders;
}

export interface KafkaMessageObject extends Message {
    value: any | Buffer | string | null;
    key: any;
}

export interface KafkaMessageSend extends Omit<ProducerRecord, 'topic'> {
    messages: KafkaMessageObject[];
    topic?: string;
    partition?: number;
}

export interface KafkaModuleOption {
    name: string;
    options: {
        client: KafkaConfig;
        consumer: ConsumerConfig;
        producer?: ProducerConfig;
          deserializer?: any;
        //   serializer?: Serializer;
        consumeFromBeginning?: boolean;
        seek?: Record<string, number | 'earliest'>;
    };
}

export enum ValidationResult {
    Pass = 'PASS',
    Fail = 'FAIL',
}


export interface MessageModel {
    id: string;
    msgType: string;
    transformedMessage: string;
    token: string;
}