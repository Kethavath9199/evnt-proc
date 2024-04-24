import { IsNotEmpty } from "class-validator";
import { MessageModel } from "../interfaces";
const decodeBase64 = require('atob');


export class MessageModelDto implements MessageModel {
    @IsNotEmpty()
    id: string = '';
    @IsNotEmpty()
    msgType: string = '';
    @IsNotEmpty()
    transformedMessage: string = '';
    token: string = '';

    decodeMessage(): string {
        return decodeBase64(this.transformedMessage);
    }
}