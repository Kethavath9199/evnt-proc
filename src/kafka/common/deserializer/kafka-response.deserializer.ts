import { plainToInstance } from 'class-transformer';
import { validateSync } from 'class-validator';
import { KafkaResponse } from '../interfaces';
import { IHeaders, KafkaMessage } from 'kafkajs';
import { ValidationResult } from '../interfaces';
import { MessageModelDto } from '../validation/messageModelSchema';

export class KafkaResponseDeserializer {
    deserialize(message: KafkaMessage): KafkaResponse {
        console.log('Incoming kafka message on deserializer.');
        const { key, value, timestamp, offset, headers } = message;

        if (!value || !Buffer.isBuffer(value)) {
            throw Error(`KakaMessage failed to Deserialize missing value/key`);
        }
        const id = key ? Buffer.from(key).toString() : '';
        const response = Buffer.from(value).toString();

        const messageData = JSON.parse(response);
        console.debug('messageData: ' + JSON.stringify(messageData));


        const businessMessageParseResult = this.parseBusinessMessage(messageData);
        if (businessMessageParseResult[0] === 'PASS') {
            return this.parsedMessage(
                id,
                businessMessageParseResult[1],
                timestamp,
                offset,
                headers,
            );
        }

        throw Error(
            `Could not parse kafka message:\n ${JSON.stringify(messageData)}`,
        );
    }


    private parseBusinessMessage(
        message: any
    ): [ValidationResult, MessageModelDto | null] {
        const businessMessage = plainToInstance(MessageModelDto, message);
        const validationResults = [];
        if (validationResults.length > 0) return [ValidationResult.Fail, null];
        return [ValidationResult.Pass, businessMessage];
    }

    private parsedMessage(
        id: string,
        messageData: any,
        timestamp: string,
        offset: string,
        headers?: IHeaders,
    ): KafkaResponse {
        return {
            key: id,
            response: messageData,
            timestamp,
            offset,
            headers,
        };
    }
}
