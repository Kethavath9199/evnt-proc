import { collectionNames } from "../utils/enumProvider";
import { DBService } from "./mongodb/mongodb.service";
import { DI } from "../di/di.container";
import { Logger } from "../utils/logger";
import axios from "axios";
import { ShipmentsService } from "./shipments.service";
import { EmailNotificationQueueProducer } from "../queue/queueProducer/emailNotificationQueue.producer";
import { ProducerService } from "../kafka/producer/producer.service";

let cargoApiConfig: any = {
    headers: {
        'Cache-Control': 'no-cache',
        'Content-Type': 'application/json',
        'X-DPW-ApiKey': process.env.CARGOES_API_KEY,
        'X-DPW-Org-Token': process.env.CARGOES_ORG_TOKEN
    },
    params: {}
}
const apiKey = process.env.GOOGLE_MAP_API_KEY;
const shipmentEventsTopic = process.env.KAFKA_TOPIC_SHIPMENT_EVENTS || '';

export class EventsService {
    private dbService: DBService;
    private logger: Logger;
    private shipmentsService: ShipmentsService;
    private emailNotificationQueueProducer: EmailNotificationQueueProducer;
    private producerService: ProducerService;
    private masterEventCache: Record<string, any> = {};
    private countries: Record<string, any> = {};

    constructor() {
        this.dbService = DI.get<DBService>(DBService);
        this.logger = DI.get<Logger>(Logger);
        this.emailNotificationQueueProducer = DI.get<EmailNotificationQueueProducer>(EmailNotificationQueueProducer);
        this.shipmentsService = DI.get<ShipmentsService>(ShipmentsService);
        this.producerService = DI.get<ProducerService>(ProducerService)
    }

    async fetchCargoEvents(shipmentQuery: any) {
        return new Promise(async (resolve, reject) => {
            try {
                cargoApiConfig.params = shipmentQuery;
                cargoApiConfig.headers = {
                    'Cache-Control': 'no-cache',
                    'Content-Type': 'application/json',
                    'X-DPW-ApiKey': process.env.CARGOES_API_KEY,
                    'X-DPW-Org-Token': process.env.CARGOES_ORG_TOKEN
                };
                const url = `${process.env.CARGOES_HOST}/shipments`;

                axios.get(url, cargoApiConfig).then((res: any) => {
                    // console.log("Response from CargoEs events Api", JSON.stringify(res.data))
                    resolve(res.data)
                }).catch((err) => {
                    this.logger.error("Error while calling Cargo events  API", url, err);
                    resolve([]);
                });
            } catch (err) {
                this.logger.error(err)
                return resolve([])
            }
        })
    }

    async persistanceOfEvents() {
        return new Promise(async (resolve, reject) => {
            try {
                const intransitShipments: any = await this.shipmentsService.fetchIntransitShipments();
                //intransit Shipments info this.logger.debug(`intransitShipments : ${JSON.stringify(intransitShipments)}`);
                for (let shipment of intransitShipments) {

                    let { awbNumber, shipmentReference, containerNumber, shipmentMode } = shipment;
                    let shipmentDetailsFromCargo: any = [];
                    let shipmentExistingEvents: any = [];
                    let shipmentExistingExceptions: any = [];
                    let createdByDetails: any;
                    let docReference: string = shipment?.carrierLines?.docReference || '';

                    // this.logger.debug(`shipment : ${JSON.stringify(shipment)}`);

                    switch (shipmentMode) {
                        case 'Ocean':
                            //fetching Ocean events from cargoes api
                            shipmentDetailsFromCargo = await this.fetchCargoEvents({
                                'shipmentType': "INTERMODAL_SHIPMENT",
                                "referenceNumber": shipmentReference
                            });

                            //fetching existing Ocean events from database
                            shipmentExistingEvents = await this.dbService.getByQuery(
                                collectionNames.collName_intransitevents,
                                {
                                    "docReference": docReference
                                }
                            );

                            //fetching existing Ocean exceptions from database
                            shipmentExistingExceptions = await this.dbService.getByQuery(
                                collectionNames.collName_exceptions,
                                {
                                    "docReference": docReference
                                }
                            );
                            break;
                        case 'Air':
                            //fetching Air events from cargoes api
                            shipmentDetailsFromCargo = await this.fetchCargoEvents({
                                'shipmentType': "AIR_SHIPMENT",
                                "awbNumber": awbNumber
                            });

                            //fetching existing Air events from database
                            shipmentExistingEvents = await this.dbService.getByQuery(
                                collectionNames.collName_intransitevents,
                                {
                                    "docReference": docReference
                                }
                            );

                            //fetching existing Air exceptions from database
                            shipmentExistingExceptions = await this.dbService.getByQuery(
                                collectionNames.collName_exceptions,
                                {
                                    "docReference": docReference
                                }
                            );
                            break;
                        default:
                            continue;
                    }

                    //shipment Details From Cargo info this.logger.debug(`shipmentDetailsFromCargo : ${JSON.stringify(shipmentDetailsFromCargo)}`);
                    // this.logger.debug(`shipmentExistingEvents : ${JSON.stringify(shipmentExistingEvents)}`);
                    // this.logger.debug(`shipmentExistingExceptions : ${JSON.stringify(shipmentExistingExceptions)}`);

                    let cargoShipmentEvents: any = [];
                    let cargoShipmentExceptions: any = [];
                    if (shipmentDetailsFromCargo.length == 0) {  //if shipment details does not exists in cargoes then skip and go to next ship
                        continue;
                    }
                    const generateShipmentAndRoute: any = await this.generateShipmentAndRoute(shipmentDetailsFromCargo[0], shipmentReference);

                    // this.logger.debug(`generateShipmentAndRoute:${JSON.stringify(generateShipmentAndRoute)}`)
                    cargoShipmentEvents = shipmentDetailsFromCargo[0].shipmentEvents || []
                    cargoShipmentExceptions = shipmentDetailsFromCargo[0].shipmentExceptions || []
                    if (cargoShipmentEvents.length == 0) {   //if shipment events does not exists in cargoes then skip and go to next ship
                        continue;
                    }

                    // cargo Shipment Events info this.logger.debug(`cargoShipmentEvents : ${JSON.stringify(cargoShipmentEvents)}`);

                    let newCargoEvents: any = cargoShipmentEvents.filter((event: any) => {  //filtering only delta (new events)
                        const eventName = event.name;
                        return !shipmentExistingEvents.some((e: any) => e.shipmentEventName === eventName);
                    });

                    let newCargoExceptions: any = cargoShipmentExceptions.filter((exception: any) => {  //filtering only delta (new exceptions)
                        const title = exception.title;
                        return !shipmentExistingExceptions.some((e: any) => e.title === title);
                    });


                    let notify: any = shipment?.notify || [];

                    createdByDetails = await this.dbService.getByQuery(
                        collectionNames.collName_partyMaster,
                        {
                            "partyId": shipment.createdBy
                        }
                    );

                    if (newCargoEvents.length > 0) {   //if  updates found in events then insert events
                        this.logger.debug(`newCargoEvents : ${JSON.stringify(newCargoEvents)}`);
                        let emailrecipients: any = notify?.map((emailDetails: any) => emailDetails.email);

                        await this.insertShipmentEvents(
                            newCargoEvents,
                            {
                                shipmentMode: shipmentMode,
                                containerNumber: containerNumber || '',
                                awbNumber: awbNumber || '',
                                shipmentReference: shipmentReference || '',
                                shipmentDetails: shipment,
                                emailReceiver: createdByDetails[0]?.partyEmail || '',
                                emailReceiverName: createdByDetails[0]?.partyName || '',
                                ccEmailrecipients: emailrecipients,
                                flightNumber: shipmentDetailsFromCargo[0].shipmentLegs?.portToPort?.currentTripNumber || '',
                                transportName: shipmentDetailsFromCargo[0].shipmentLegs?.portToPort?.currentTransportName || '',
                                // origin: shipmentDetailsFromCargo[0].shipmentLegs?.portToPort?.loadingPort || shipmentDetailsFromCargo[0].shipmentLegs?.portToPort?.originPort || '',
                                origin: generateShipmentAndRoute?.route?.originDetails?.location || '',
                                // originCode: shipmentDetailsFromCargo[0].shipmentLegs?.portToPort?.loadingPortCode || shipmentDetailsFromCargo[0].shipmentLegs?.portToPort?.originPortCode || '',
                                originCode: generateShipmentAndRoute?.route?.originDetails?.locationCode || '',
                                // destination: shipmentDetailsFromCargo[0].shipmentLegs?.portToPort?.dischargePort || shipmentDetailsFromCargo[0].shipmentLegs?.portToPort?.destinationPort || '',
                                destination: generateShipmentAndRoute?.route?.destinationDetails?.location || '',
                                // destinationCode: shipmentDetailsFromCargo[0].shipmentLegs?.portToPort?.dischargePortCode || shipmentDetailsFromCargo[0].shipmentLegs?.portToPort?.destinationPortCode || '',
                                destinationCode: generateShipmentAndRoute?.route?.destinationDetails?.locationCode || '',
                                eta: generateShipmentAndRoute?.route?.destinationDetails?.eta || '',
                                generateShipmentAndRoute: generateShipmentAndRoute
                            }
                        )
                    }

                    if (newCargoExceptions.length > 0) {   //if  updates found in exceptions then insert events
                        this.logger.debug(`newCargoExceptions : ${JSON.stringify(newCargoExceptions)}`);
                        let emailrecipients: any = notify?.map((emailDetails: any) => emailDetails.email);

                        await this.insertShipmentExceptions(
                            newCargoExceptions,
                            {
                                shipmentMode: shipmentMode,
                                containerNumber: containerNumber || '',
                                awbNumber: awbNumber || '',
                                emailReceiver: createdByDetails[0]?.partyEmail || '',
                                emailReceiverName: createdByDetails[0]?.partyName || '',
                                shipmentReference: shipmentReference || '',
                                shipmentDetails: shipment,
                                ccEmailrecipients: emailrecipients,
                                origin: generateShipmentAndRoute?.route?.originDetails?.location || '',
                                destination: generateShipmentAndRoute?.route?.destinationDetails?.location || '',
                                // partyDetails: createdByDetails
                            }
                        )
                    }

                }
                return resolve(`Events are persisted successfully`)
            } catch (err) {
                return resolve(err);
            }
        })
    }

    async insertShipmentEvents(data: any, shipmentInfo: any) {
        return new Promise(async (resolve, reject) => {

            try {

                let {
                    shipmentMode,
                    containerNumber,
                    awbNumber,
                    shipmentReference,
                    shipmentDetails,
                    emailReceiver,
                    ccEmailrecipients,
                    flightNumber,
                    transportName,
                    origin,
                    originCode,
                    destination,
                    destinationCode,
                    eta,
                    generateShipmentAndRoute
                } = shipmentInfo;

                let allEvents: any = [];
                let checkGateOutEventExistance: boolean = false;
                let checkPickupEventExistance: boolean = false;
                let pickupDate: any = '';
                let eventLastUpdatedDateExist: boolean = false;
                let eventLastUpdatedDate: Date;
                let lastEventName: string;
                let eventContentForEmailNotification: any = {
                    notificationType: 'events',
                    receiverEmail: emailReceiver || '',
                    ccEmailrecipients: [],
                    origin: origin || '',
                    via: '',
                    destination: destination || '',
                    shipmentType: shipmentMode,
                    shipperName: shipmentDetails?.carrierHeaders?.shipperName || '',
                    consigneeName: shipmentDetails?.carrierHeaders?.consigneeName || '',
                    shipmentNumber: containerNumber || awbNumber,
                    mblNumber: shipmentDetails?.carrierLines?.mblNumber || '',
                    shipmentReference: shipmentReference,
                    // receiverEmail: shipmentDetails?.notify?.map((obj: any) => obj.email) || [],
                    data: []
                }

                // this.logger.debug(`shipmentDetails?.carrierLines`, shipmentDetails?.carrierLines);
                for (let event of data) {

                    let actualTime: any = event?.actualTime || '';
                    let estimateTime: any = event?.estimateTime || '';
                    let masterEvents: any = {};
                    let fullContainerGateInAtDestination: boolean = false;
                    let locationType: any;

                    //deriving locationType based on locationCode
                    if (event.locationCode === generateShipmentAndRoute?.route?.originDetails?.locationCode) {
                        locationType = 'originPort';
                    } else if (event.locationCode === generateShipmentAndRoute?.route?.destinationDetails?.locationCode) {
                        locationType = 'destinationPort';
                    } else {
                        const transhipment = generateShipmentAndRoute?.route?.transhipmentDetails.find(
                            (transhipment: any) => transhipment.locationCode === event.locationCode
                        );
                        locationType = transhipment ? 'transhipmentPort' : 'preCarriagePort';
                    }

                    if (locationType == 'destinationPort' && event.name == 'Full container gate in') {
                        fullContainerGateInAtDestination = true;
                    }
                    if (this.masterEventCache[event.name]) {
                        // Event is in cache,  return the cached response
                        masterEvents = this.masterEventCache[event.name];
                    } else {
                        // Event is not in cache, fetch the response from the database
                        const response: any = await this.dbService.getByQuery(
                            collectionNames.collectionName_masterEvents,
                            {
                                "eventName": event.name
                            }
                        );

                        if (response.length == 0) {
                            continue;
                        }
                        // Store the response in the cache
                        this.masterEventCache[event.name] = response[0]
                        masterEvents = response[0]
                    }

                    let dataObj: any = {
                        docReference: shipmentDetails?.carrierLines?.docReference,
                        bookingReferenceId: shipmentDetails?.carrierLines?.bookingReference || '',
                        carrierReferenceId: shipmentDetails?.carrierLines?.carrierReference || '',
                        containerId: containerNumber,
                        awbNumber: awbNumber,
                        shipmentReference: shipmentReference,
                        carrierID: shipmentDetails?.carrierLines?.carrierId || '',
                        carrierName: shipmentDetails?.carrierLines?.carrierName || '',
                        forwaderID: shipmentDetails?.carrierLines?.createdBy,
                        forwaderName: shipmentDetails?.createdByName,
                        shipperID: shipmentDetails?.carrierHeaders?.shipperId || '',
                        shipperName: shipmentDetails?.carrierHeaders?.shipperName || '',
                        consigneeID: shipmentDetails?.carrierHeaders?.consigneeId || '',
                        consigneeName: shipmentDetails?.carrierHeaders?.consigneeName || '',
                        origin: origin || '',
                        originCode: originCode || '',
                        destination: destination || '',
                        destinationCode: destinationCode || '',
                        hscode: shipmentDetails?.carrierLines?.hscode || '',
                        invoiceId: shipmentDetails?.carrierLines?.invoiceNumber,
                        shipmentMode: shipmentMode,
                        hawbNumber: shipmentDetails?.carrierLines?.hawbNumber || '',
                        mawbNumber: shipmentDetails?.carrierLines?.awbNumber || '',
                        HBLId: shipmentDetails?.carrierLines?.hblNumber || '',
                        MBLId: shipmentDetails?.carrierLines?.mblNumber || '',
                        vesselID: shipmentDetails?.carrierLines?.vesselId || '',
                        vesselName: shipmentDetails?.carrierLines?.vesselName || '',
                        voyageId: shipmentDetails?.carrierLines?.voyage || '',
                        flightNumber: flightNumber,
                        transportName: transportName,
                        shipmentEventName: event.name,
                        eventLocation: event.location,
                        eventLocationCode: event.locationCode,
                        hasCargo: event.hasCargo,
                        eventMapping: fullContainerGateInAtDestination ? 'Gate Out - Destination' : masterEvents?.eventMapping || '',
                        eventGroup: fullContainerGateInAtDestination ? 'On Carriage' : masterEvents?.eventGroup || '',
                        shipmentEventDate: (actualTime != '' || estimateTime != '') ? new Date(event?.actualTime || event?.estimateTime) : '',
                        eventActualTime: actualTime != '' ? new Date(event?.actualTime) : '',
                        eventEstimatedTime: estimateTime != '' ? new Date(event?.estimateTime) : '',
                        createdBy: shipmentDetails?.createdBy,
                        modifiedBy: shipmentDetails?.createdBy,
                        createdDate: await this.getCurrentDate(),
                        modifiedDate: await this.getCurrentDate()
                    }
                    if (dataObj.eventActualTime != '') {
                        eventLastUpdatedDateExist = true;
                        eventLastUpdatedDate = dataObj.eventActualTime
                        lastEventName = dataObj.shipmentEventName
                    }

                    allEvents.push(dataObj);
                    eventContentForEmailNotification.data.push({
                        eventName: event?.name || '',
                        eventCode: event?.code || '',
                        location: event?.location || 'NA',
                        locationCode: event?.locationCode || 'NA',
                        date: new Date(event?.actualTime || event?.estimateTime),
                        ata: actualTime != '' ? new Date(event?.actualTime) : 'NA',
                        eta: estimateTime != '' ? new Date(event?.estimateTime) : 'NA',
                        changeInEta: event?.changeInEta ?? 'NA'
                    })

                    const regexOcean = /Full Container Gate In|Full Container Available For Delivery|Full Container Dropoff by Truck|Empty Container Dropoff by Truck|Container Dropoff by Truck|Empty Container Gate In/gi;
                    const regexAir = /Delivered/gi;
                    const regexPickupOcean = /Vessel Departure/gi;
                    const regexPickupAir = /Received from Shipper/gi;

                    if (typeof event?.name === 'string') {
                        const matchesOcean = event.name.match(regexOcean);
                        const matchesAir = event.name.match(regexAir);
                        const matchesPickupOcean = event.name.match(regexPickupOcean);
                        const matchesPickupAir = event.name.match(regexPickupAir);
                        // this.logger.debug(`matchesOcean : ${matchesOcean} , matchesAir: ${matchesAir},matchesPickupOcean:${matchesPickupOcean} , matchesPickupAir:${matchesPickupAir}`);
                        if (matchesOcean && masterEvents.eventGroup == 'On Carriage' || shipmentMode == 'Air' && matchesAir) {
                            if (event.name.match(/Full Container Gate In/gi) && destinationCode != event?.locationCode) {
                                checkGateOutEventExistance = false;
                            } else {
                                checkGateOutEventExistance = true;
                            }
                        }

                        if (matchesPickupOcean && shipmentMode == 'Ocean' || matchesPickupAir && event?.locationCode == generateShipmentAndRoute?.route?.originDetails?.locationCode) {
                            checkPickupEventExistance = true;
                            pickupDate = (actualTime != '' || estimateTime != '') ? new Date(event?.actualTime || event?.estimateTime) : ''
                        }
                    }

                }
                this.logger.debug(`allEvents data for insertion : ${JSON.stringify(allEvents)}`);

                if (allEvents.length == 0) return resolve(`No events`);


                await this.dbService.insertMultipleData(
                    collectionNames.collName_intransitevents,
                    allEvents
                );

                const transportNumber = shipmentMode == 'Ocean' ? containerNumber : awbNumber
                const partition = this.getPartition(transportNumber, 5)

                //post event to kafka
                this.postEvent('shipmentEvent', partition, transportNumber, allEvents);

                eta = eta == '' ?? new Date(eta) < new Date('01-01-2000') ? '' : new Date(eta);
                const oldEta = shipmentDetails?.carrierLines?.eta || '';  //checking 1970 dates if occurs
                const etaExistance = oldEta != '' ? new Date(oldEta) < new Date('01-01-2000') ? false : true : false;
                const changeInEtaExistance = shipmentDetails?.carrierLines?.changeInEta || false;
                this.logger.debug(`eta:${eta}, etaExistance : ${etaExistance} , changeInEtaExistance: ${changeInEtaExistance}`)
                if (!etaExistance && !changeInEtaExistance) {
                    const objData = {
                        eta: eta,
                        changeInEta: eta
                    }
                    await this.dbService.updateMany(
                        collectionNames.collName_carrierBookingLines,
                        {
                            $set: objData
                        },
                        {
                            shipmentReference: shipmentReference
                        }
                    )

                    this.postEtaChangeInEta(
                        'eta-changeInEta',
                        partition,
                        transportNumber,
                        objData
                    )

                }
                else if (!changeInEtaExistance) {
                    await this.dbService.updateMany(
                        collectionNames.collName_carrierBookingLines,
                        {
                            $set: {
                                changeInEta: eta
                            }
                        },
                        {
                            shipmentReference: shipmentReference
                        }
                    )

                    this.postEtaChangeInEta(
                        'changeInEta',
                        partition,
                        transportNumber,
                        { changeInEta: eta }
                    )
                } else if (!etaExistance) {
                    await this.dbService.updateMany(
                        collectionNames.collName_carrierBookingLines,
                        {
                            $set: {
                                eta: eta
                            }
                        },
                        {
                            shipmentReference: shipmentReference
                        }
                    )

                    this.postEtaChangeInEta(
                        'eta',
                        partition,
                        transportNumber,
                        { eta: eta }
                    )
                }

                if (checkPickupEventExistance && pickupDate != '') {
                    await this.dbService.updateMany(
                        collectionNames.collName_carrierBookingLines,
                        {
                            $set: {
                                pickUpDate: pickupDate || new Date(pickupDate)
                            }
                        },
                        {
                            shipmentReference: shipmentReference
                        }
                    )
                }

                if (eventLastUpdatedDateExist) {
                    //update last event updated at if exist
                    await this.dbService.updateMany(
                        collectionNames.collName_carrierBookingLines,
                        {
                            $set: {
                                lastEventUpdatedAt: eventLastUpdatedDate!,
                                lastEventName: lastEventName!
                            }
                        },
                        {
                            shipmentReference: shipmentReference
                        }
                    )
                }


                // this.logger.debug(`eventContentForEmailNotification:${JSON.stringify(eventContentForEmailNotification)}`)
                if (eventContentForEmailNotification.data.length > 0) {

                    //email for main party (the party who registred shipment)   
                    eventContentForEmailNotification.userRegistered = true;
                    console.log(`eventContentForEmailNotification for main email: ${JSON.stringify(eventContentForEmailNotification)}`)
                    await this.emailNotificationQueueProducer.addJobToEmailNotificationQueue(eventContentForEmailNotification);

                    //email for notifiers
                    for (let i = 0; i < ccEmailrecipients.length; i++) {
                        const emailId = ccEmailrecipients[i];
                        if (emailId == emailReceiver) continue;
                        const checkUserRegisteredByEmail: any = await this.dbService.getByQuery(collectionNames.collName_partyMaster,
                            {
                                partyEmail: emailId
                            })
                        const userRegistered = checkUserRegisteredByEmail.length > 0 ? true : false;
                        eventContentForEmailNotification.receiverEmail = emailId
                        eventContentForEmailNotification.ccEmailrecipients = []
                        eventContentForEmailNotification.userRegistered = userRegistered;
                        console.log(`eventContentForEmailNotification for  notify email: ${JSON.stringify(eventContentForEmailNotification)}`)
                        await this.emailNotificationQueueProducer.addJobToEmailNotificationQueue(eventContentForEmailNotification);
                    }
                }


                if (checkGateOutEventExistance) {
                    console.log(shipmentReference, ' :entered into checkGateOutEventExistance to update gateOut event: ', checkGateOutEventExistance)
                    let query: any = {
                        shipmentReference: shipmentReference
                    }
                    let data: any = {
                        shipmentStatus: "gateOut"
                    }

                    await this.dbService.updateAllDocumentsByQuery(collectionNames.collName_carrierBookingLines,
                        data,
                        query
                    )
                }
                this.logger.debug(`all events inserted successfully`);

                return resolve(`updated events got inserted successfully`)
            } catch (err) {
                this.logger.error(err)
                return resolve(err);
            }
        })
    }

    async insertShipmentExceptions(data: any, shipmentInfo: any) {
        return new Promise(async (resolve, reject) => {

            try {

                let {
                    shipmentMode,
                    containerNumber,
                    awbNumber,
                    emailReceiver,
                    emailReceiverName,
                    shipmentReference,
                    shipmentDetails,
                    ccEmailrecipients,
                    origin,
                    destination
                } = shipmentInfo;

                let allExceptions: any = [];
                let latestEtaDescriptionFromExceptions: any;
                let checkEtaChangeExceptionExistance: boolean = false;
                let exceptionContentForEmailNotification: any = {
                    shipmentType: shipmentMode,
                    notificationType: 'exceptions',
                    transportNumber: containerNumber || '',
                    receiverEmail: emailReceiver || '',
                    origin: origin || '',
                    via: '',
                    destination: destination || '',
                    emailReceiverName: emailReceiverName || '',
                    ccEmailrecipients: [],
                    shipperName: shipmentDetails?.carrierHeaders?.shipperName || '',
                    consigneeName: shipmentDetails?.carrierHeaders?.consigneeName || '',
                    shipmentNumber: containerNumber || awbNumber,
                    mblNumber: shipmentDetails?.carrierLines?.mblNumber || '',
                    shipmentReference: shipmentReference,
                    data: []
                }

                for (let exception of data) {

                    let dataObj: any = {
                        docReference: shipmentDetails.carrierLines.docReference,
                        severity: 1,
                        createdAt: new Date(exception.createdAt),
                        exceptionType: exception.exceptionType,
                        description: exception.description,
                        title: exception.title,
                        awbNumber: awbNumber,
                        shipmentReference: shipmentReference,
                        shipmentMode: shipmentMode,
                        createdBy: shipmentDetails.createdBy,
                        modifiedBy: shipmentDetails.createdBy,
                        createdDate: new Date(),
                        modifiedDate: new Date()
                    }

                    allExceptions.push(dataObj);

                    let eventLatestEtaDescriptionFromExceptions: any = '';
                    if (!checkEtaChangeExceptionExistance) {
                        const regex = exception.title.match(new RegExp(`\\bETA changed\\b`, 'gi'))
                        if (regex) {
                            checkEtaChangeExceptionExistance = true;
                            latestEtaDescriptionFromExceptions = exception.description;
                            eventLatestEtaDescriptionFromExceptions = latestEtaDescriptionFromExceptions.match(/(\d{4}-\d{2}-\d{2})/g);
                            eventLatestEtaDescriptionFromExceptions = eventLatestEtaDescriptionFromExceptions[1]
                        }
                    }

                    exceptionContentForEmailNotification.data.push({
                        exceptionTitle: exception.title,
                        exceptionDescription: exception.description,
                        date: new Date(exception.createdAt),
                        changeInEta: eventLatestEtaDescriptionFromExceptions == '' ?? new Date(),
                        eta: eventLatestEtaDescriptionFromExceptions == '' ?? new Date(),
                    })

                }

                this.logger.debug(`allEvents data for insertion : ${JSON.stringify(allExceptions)}`);

                if (allExceptions.length == 0) return resolve(`No exceptions`)

                await this.dbService.insertMultipleData(
                    collectionNames.collName_exceptions,
                    allExceptions
                );

                if (checkEtaChangeExceptionExistance) {
                    latestEtaDescriptionFromExceptions = latestEtaDescriptionFromExceptions.match(/(\d{4}-\d{2}-\d{2})/g);
                    let query: any = {
                        shipmentReference: shipmentReference
                    }
                    let data: any = {
                        changeInEta: new Date(latestEtaDescriptionFromExceptions[1])
                    }

                    await this.dbService.updateAllDocumentsByQuery(collectionNames.collName_carrierBookingLines,
                        data,
                        query
                    )
                }

                if (exceptionContentForEmailNotification.data.length > 0) {
                    await this.emailNotificationQueueProducer.addJobToEmailNotificationQueue(exceptionContentForEmailNotification);
                }
                if (exceptionContentForEmailNotification.data.length > 0) {

                    //email for main party (the party who registred shipment)   
                    exceptionContentForEmailNotification.userRegistered = true;
                    console.log(`eventContentForEmailNotification for main email: ${JSON.stringify(exceptionContentForEmailNotification)}`)
                    await this.emailNotificationQueueProducer.addJobToEmailNotificationQueue(exceptionContentForEmailNotification);

                    //email for notifiers
                    for (let i = 0; i < ccEmailrecipients.length; i++) {
                        const emailId = ccEmailrecipients[i];
                        if (emailId == emailReceiver) continue;
                        const checkUserRegisteredByEmail: any = await this.dbService.getByQuery(collectionNames.collName_partyMaster,
                            {
                                partyEmail: emailId
                            })
                        const userRegistered = checkUserRegisteredByEmail.length > 0 ? true : false;
                        exceptionContentForEmailNotification.receiverEmail = emailId
                        exceptionContentForEmailNotification.ccEmailrecipients = []
                        exceptionContentForEmailNotification.userRegistered = userRegistered;
                        console.log(`eventContentForEmailNotification for  notify email: ${JSON.stringify(exceptionContentForEmailNotification)}`)
                        await this.emailNotificationQueueProducer.addJobToEmailNotificationQueue(exceptionContentForEmailNotification);
                    }
                }

                this.logger.debug(`all events inserted successfully`);

                return resolve(`updated events got inserted successfully`)
            } catch (err) {
                this.logger.error(err)
                return resolve(err);
            }
        })
    }

    async generateShipmentAndRoute(cargoData: any, shipmentReference: any) {
        return new Promise(async (resolve, reject) => {
            try {
                this.logger.debug(`function call :generateShipmentAndRoute for shipmentReference:${shipmentReference}`)
                let originCountryDetails: any;
                let designationCountryDetails: any;

                const shipmentDetails: any = {
                    shipper: cargoData.shipper || '',
                    consignee: cargoData.consignee || '',
                    shipmentNumber: cargoData.shipmentNumber || '',
                    containerNumber: cargoData.containerNumber || '',
                    containerSealNumber: cargoData.containerSealNumber || '',
                    containerSize: cargoData.containerSize || '',
                    containerType: cargoData.containerType || '',
                    containerIso: cargoData.containerIso || '',
                    referenceNumber: cargoData.referenceNumber || '',
                    status: cargoData.status || '',
                    subStatus1: cargoData.subStatus1 || '',
                    subStatus2: cargoData.subStatus2 || '',
                    shippingMode: cargoData.shippingMode || '',
                    awbNumber: cargoData.awbNumber || '',
                    hblNumber: cargoData.hblNumber || '',
                    mblNumber: cargoData.mblNumber || '',
                    cuurentFlightNumber: cargoData?.shipmentLegs?.portToPort?.currentTripNumber || '',
                    cuurentTransportName: cargoData?.shipmentLegs?.portToPort?.currentTransportName || '',
                    carrier: cargoData?.shipmentLegs?.portToPort?.carrier || '',
                    bookingNumber: cargoData.bookingNumber || '',
                    createdAt: cargoData.createdAt && cargoData.createdAt !== '' ? new Date(cargoData.createdAt) : '',
                    serviceMode: cargoData.serviceMode || '',
                    promisedEta: cargoData.promisedEta || '',
                    incoterm: cargoData.incoterm || '',
                    totalWeight: cargoData.totalWeight || '',
                    totalWeightUom: cargoData.totalWeightUom || '',
                    netWeight: cargoData.netWeight || '',
                    netWeightUom: cargoData.netWeightUom || '',
                    totalNumberOfPackages: cargoData.totalNumberOfPackages || '',
                    packageType: cargoData.packageType || '',
                    commodity: cargoData.commodity || '',
                    trackingUpdatedAt: (cargoData.trackingUpdatedAt && cargoData.trackingUpdatedAt != '') ? new Date(cargoData.trackingUpdatedAt) : '',
                    currentLocationName: cargoData.currentLocationName || '',
                    currentLocationUpdatedAt: (cargoData.currentLocationUpdatedAt && cargoData.currentLocationUpdatedAt != '') ? new Date(cargoData.currentLocationUpdatedAt) : '',
                    totalVolume: cargoData.totalVolume || '',
                    totalVolumeUom: cargoData.totalVolumeUom || '',
                    carrierScac: cargoData.carrierScac || '',
                    emissions: cargoData.emissions || '',
                }

                const originCode: string = cargoData?.shipmentLegs?.portToPort?.originPortCode ||
                    cargoData?.shipmentLegs?.portToPort?.loadingPortCode || '';

                const origin: string = cargoData?.shipmentLegs?.portToPort?.originPort ||
                    cargoData?.shipmentLegs?.portToPort?.loadingPort || '';

                const originEtd: string = cargoData?.shipmentLegs?.portToPort?.originPortEtd ||
                    cargoData?.shipmentLegs?.portToPort?.loadingPortEtd || '';

                const originAtd: string = cargoData?.shipmentLegs?.portToPort?.originPortAtd ||
                    cargoData?.shipmentLegs?.portToPort?.loadingPortAtd || '';

                let destinationCode: string = cargoData?.shipmentLegs?.portToPort?.destinationPortCode ||
                    cargoData?.shipmentLegs?.portToPort?.dischargePortCode || '';

                let destination: string = cargoData?.shipmentLegs?.portToPort?.destinationPort ||
                    cargoData?.shipmentLegs?.portToPort?.dischargePort || '';

                const destinationEta: string = cargoData?.shipmentLegs?.portToPort?.destinationPortEta ||
                    cargoData?.shipmentLegs?.portToPort?.dischargePortEta || '';

                const destinationAta: string = cargoData?.shipmentLegs?.portToPort?.destinationPortAta ||
                    cargoData?.shipmentLegs?.portToPort?.dischargePortAta || '';

                let route = {
                    originDetails: {
                        locationCode: originCode,
                        location: origin,
                        etd: originEtd != '' ? new Date(originEtd) : '',
                        atd: originAtd != '' ? new Date(originAtd) : '',
                        flightNumber: cargoData?.shipmentLegs?.portToPort?.segments.find(
                            (segment: any) => segment.originPortCode === originCode
                        )?.tripNumber || '',
                        countryShortName: '',
                        countryLongName: '',
                        postalCode: '',
                        lat: '',
                        lng: '',
                        address: ''
                    },
                    destinationDetails: {
                        locationCode: destinationCode,
                        location: destination,
                        eta: destinationEta != '' ? new Date(destinationEta) : '',
                        ata: destinationAta != '' ? new Date(destinationAta) : '',
                        flightNumber: cargoData?.shipmentLegs?.portToPort?.segments.find(
                            (segment: any) => segment.destinationPortCode === destinationCode
                        )?.tripNumber || '',
                        countryShortName: '',
                        countryLongName: '',
                        postalCode: '',
                        lat: '',
                        lng: '',
                        address: ''
                    },
                    transhipmentDetails: [] as any[],
                };
                // this.logger.log(`route : ${JSON.stringify(route)}`)

                if (cargoData?.shipmentLegs?.portToPort?.segments.length > 0 && destinationCode == '') {
                    let lastIndex: number = cargoData?.shipmentLegs?.portToPort?.segments.length - 1;
                    let eta: any = cargoData?.shipmentLegs?.portToPort?.segments[lastIndex]?.eta || '';
                    let ata: any = cargoData?.shipmentLegs?.portToPort?.segments[lastIndex]?.ata || '';
                    destinationCode = cargoData?.shipmentLegs?.portToPort?.segments[lastIndex]?.destinationPortCode || ''
                    destination = cargoData?.shipmentLegs?.portToPort?.segments[lastIndex]?.destination || ''
                    route.destinationDetails.locationCode = cargoData?.shipmentLegs?.portToPort?.segments[lastIndex]?.destinationPortCode || ''
                    route.destinationDetails.location = cargoData?.shipmentLegs?.portToPort?.segments[lastIndex]?.destination || ''
                    route.destinationDetails.eta = eta != '' ? new Date(eta) : '';
                    route.destinationDetails.ata = ata != '' ? new Date(ata) : '';
                    route.destinationDetails.flightNumber = cargoData?.shipmentLegs?.portToPort?.segments[lastIndex]?.tripNumber || ''
                } else {
                    // Identify transhipment locations
                    for (const segment of cargoData?.shipmentLegs?.portToPort?.segments) {
                        if (segment.originPortCode !== route.originDetails.locationCode && segment.originPortCode !== route.destinationDetails.locationCode) {
                            // this.logger.log(`updated route : ${JSON.stringify(route)}`)
                            let transhipmentPortDetails: any;
                            let transhipmentcountryShortName: any = '';
                            let transhipmentcountryLongName: any = '';
                            let transhipmentpostalCode: any = '';
                            let transhipmentlat: any = '';
                            let transhipmentlng: any = '';
                            let transhipmentaddress: any = '';
                            if (this.countries[`${segment.origin}`]) {
                                transhipmentPortDetails = this.countries[`${segment.origin}`]

                            } else {
                                // originCountryDetails = await this.getCountry(originLatitide, originLongitude);
                                transhipmentPortDetails = await this.getCountry({ location: origin });
                                this.logger.debug(`transhipmentPortDetails:${JSON.stringify(transhipmentPortDetails)}`)
                                if (transhipmentPortDetails) {
                                    transhipmentcountryShortName = transhipmentPortDetails?.countryShortName || ''
                                    transhipmentcountryLongName = transhipmentPortDetails?.countryLongName || ''
                                    transhipmentpostalCode = transhipmentPortDetails?.postalCode || ''
                                    transhipmentlat = transhipmentPortDetails?.lat || ''
                                    transhipmentlng = transhipmentPortDetails?.lng || ''
                                    transhipmentaddress = transhipmentPortDetails?.address || ''
                                    this.countries[`${segment.origin}`] = transhipmentPortDetails;
                                }
                            }
                            route.transhipmentDetails.push({
                                locationCode: segment?.originPortCode || '',
                                location: segment?.origin || '',
                                eta: segment?.eta && segment?.eta != '' ? new Date(segment?.eta) : '',
                                ata: segment?.ata && segment?.ata != '' ? new Date(segment?.ata) : '',
                                flightNumber: segment?.tripNumber || '',
                                countryShortName: transhipmentcountryShortName,
                                countryLongName: transhipmentcountryLongName,
                                postalCode: transhipmentpostalCode,
                                lat: transhipmentlat,
                                lng: transhipmentlng,
                                address: transhipmentaddress
                            });
                        }
                    }
                }
                // this.logger.log(`updated route : ${JSON.stringify(route)}`)
                if (this.countries[`${origin}`]) {
                    originCountryDetails = this.countries[`${origin}`]

                } else {
                    // originCountryDetails = await this.getCountry(originLatitide, originLongitude);
                    originCountryDetails = await this.getCountry({ location: origin });
                    this.logger.debug(`originCountryDetails:${JSON.stringify(originCountryDetails)}`)
                    if (originCountryDetails) {
                        route.originDetails.countryShortName = originCountryDetails?.countryShortName || ''
                        route.originDetails.countryLongName = originCountryDetails?.countryLongName || ''
                        route.originDetails.postalCode = originCountryDetails?.postalCode || ''
                        route.originDetails.lat = originCountryDetails?.lat || ''
                        route.originDetails.lng = originCountryDetails?.lng || ''
                        route.originDetails.address = originCountryDetails?.address || ''
                        this.countries[`${origin}`] = originCountryDetails;
                    }
                }

                if (this.countries[`${destination}`]) {
                    designationCountryDetails = this.countries[`${destination}`]
                } else {
                    // designationCountryDetails = await this.getCountry(destinationLatitude, designationLongitude);
                    designationCountryDetails = await this.getCountry({ location: destination });
                    this.logger.debug(`designationCountryDetails:${JSON.stringify(designationCountryDetails)}`)
                    if (designationCountryDetails) {
                        route.destinationDetails.countryShortName = designationCountryDetails?.countryShortName || ''
                        route.destinationDetails.countryLongName = designationCountryDetails?.countryLongName || ''
                        route.destinationDetails.postalCode = designationCountryDetails?.postalCode || ''
                        route.destinationDetails.lat = designationCountryDetails?.lat || ''
                        route.destinationDetails.lng = designationCountryDetails?.lng || ''
                        route.destinationDetails.address = designationCountryDetails?.address || ''
                        this.countries[`${destination}`] = designationCountryDetails;
                    }
                }

                await this.dbService.updateMany(
                    collectionNames.collName_carrierBookingLines,
                    {
                        $set: {
                            route: route,
                            shipmentDetails: shipmentDetails,
                            carrier: shipmentDetails.carrier || ''
                        }
                    },
                    {
                        shipmentReference: shipmentReference
                    }
                )

                return resolve({ shipmentDetails, route })
            } catch (err) {
                let shipmentDetails: any = {}, route: any = {};
                return resolve({ shipmentDetails, route })
            }
        })
    }

    //google map api to fetch country using lat and lng
    async getCountry(data: any): Promise<any> {
        return new Promise(async (resolve, reject) => {
            try {

                this.logger.debug(`data:${data}`);
                let countryLongName: any = '';
                let countryShortName: any = '';
                let postalCode: any = '';
                let lat: any = ''
                let lng: any = ''
                let address: string = '';
                this.logger.debug(`Calling google map api`)
                // const apiUrl = `https://maps.googleapis.com/maps/api/geocode/json?latlng=${lat},${lng}&key=${apiKey}`;
                const apiUrl = `https://maps.googleapis.com/maps/api/geocode/json?key=${apiKey}&address=${data.location}`;
                axios.get(apiUrl).then((res: any) => {
                    console.log("Response from google map Api", JSON.stringify(res?.data?.results))
                    const results: any = res?.data?.results || [];
                    if (results.length > 0) {
                        lat = results[0]?.geometry?.location?.lat || '';
                        lng = results[0]?.geometry?.location?.lng || '';
                        address = results[0]?.formatted_address || '';
                        for (const component of results[0]?.address_components) {
                            if (component.types.includes('country')) {
                                countryLongName = component.long_name || '';
                                countryShortName = component.short_name || '';
                                // console.log(`countryLongName: ${countryLongName}`);
                                // console.log(`countryShortName: ${countryShortName}`);
                            }
                            if (component.types.includes('postal_code')) {
                                postalCode = component.long_name || '';
                                // console.log(`postalCode: ${postalCode}`);
                            }
                        }
                    }
                    return resolve({ countryLongName, countryShortName, postalCode, address, lat, lng })
                }).catch((err: any) => {
                    this.logger.error("Error while calling google map API", apiUrl, err);
                    return resolve({ countryLongName, countryShortName, postalCode, address, lat, lng });
                });

            } catch (err) {
                return reject(err)
            }
        })
    }


    async getCurrentDate() {
        return new Promise(async (resolve, reject) => {
            try {
                let dateString = new Date().toLocaleString(undefined, { timeZone: 'Asia/Kolkata', hour12: false });
                let date = new Date(dateString)
                let localDate = `${date.getFullYear()}-
                ${date.getMonth() < 9 ? "0" + (date.getMonth() + 1) : (date.getMonth() + 1)}-
                ${date.getDate() < 10 ? "0" + date.getDate() : date.getDate()}`

                return resolve(new Date(localDate));
            }
            catch (e) {
                this.logger.error(e)
                return resolve(e)
            }

        })

    }

    async postEvent(
        type: string,
        partition: number,
        transportNumber: any,
        events: any
    ): Promise<void> {
        try {
            const sortedEvents = await this.sortEvents(events);
            if (sortedEvents.length == 0)
                return;
            const event = {
                transportNumber: transportNumber,
                mode: sortedEvents[0].shipmentMode,
                eventName: sortedEvents[0].shipmentEventName,
                eventMapping: sortedEvents[0].eventMapping,
                eventGroup: sortedEvents[0].eventGroup,
                eventDate: sortedEvents[0].shipmentEventDate,
                eventEstimatedTime: sortedEvents[0].eventEstimatedTime,
                eventActualTime: sortedEvents[0].eventActualTime
            }

            const message = {
                msgType: 'shipmentEvents',
                type: type,
                value: {
                    ...event
                }
            }

            await this.producerService.postToPartition(
                shipmentEventsTopic,
                partition,
                JSON.stringify(message)
            );

        } catch (err) {
            this.logger.error(`error posting events to kafka err:${err}`)
        }
    }

    async postEtaChangeInEta(
        type: string,
        partition: number,
        transportNumber: string,
        etaobj: any
    ): Promise<void> {
        try {
            const message = {
                msgType: 'shipmentEvents',
                type: type,
                value: {
                    ...etaobj,
                    transportNumber: transportNumber
                }
            }

            await this.producerService.postToPartition(
                shipmentEventsTopic,
                partition,
                JSON.stringify(message)
            );
        } catch (err) {
            this.logger.error(`error posting events to kafka err:${err}`)
        }
    }

    sortEvents(events: any) {
        if (events.length == 0)
            return [];

        const filteredArray = events.filter((obj: any) => obj.date);

        if (filteredArray.length == 0)
            return [];

        return filteredArray.sort((a: any, b: any) => b.date - a.date);  //desc
    }

    hashCode(str: any) {
        let hash = 0;
        if (str.length === 0) return hash;
        for (let i = 0; i < str.length; i++) {
            const char = str.charCodeAt(i);
            hash = ((hash << 5) - hash) + char;
            hash |= 0; // Convert to 32bit integer
        }
        return hash;
    }

    getPartition(str: string, maxNoOfPartition: number): number {
        return Math.abs(this.hashCode(str) % maxNoOfPartition)
    }

}