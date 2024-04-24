// export const SUBSCRIBER_MAP = new Map();
export const SUBSCRIBER_OBJECT_MAP = new Map();
export const SCHEMAS = new Map();

// export function SubscribeTo(topic: string) {
//     return (
//         target: any,
//         propertyKey: string | number,
//         descriptor: any,
//     ) => {
//         console.log(`descriptor:${JSON.stringify(descriptor)}`)
//         console.log(`propertyKey:${propertyKey}`)
//         console.log(`target:${JSON.stringify(target)}`)
//         console.log(`entered into SUBSCRIBER_MAP decorator with topic :${topic}`)
//         const originalMethod = target[propertyKey];
//         console.log(`originalMethod:${JSON.stringify(originalMethod)}`)
//         SUBSCRIBER_MAP.set(topic, originalMethod);
//         console.log(` SUBSCRIBER_MAP.keys() : ${JSON.stringify(SUBSCRIBER_MAP.keys())}`)
//         return descriptor;
//     };
// }
