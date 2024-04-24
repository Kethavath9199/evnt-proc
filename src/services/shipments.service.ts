import { collectionNames } from "../utils/enumProvider";
import { DBService } from "./mongodb/mongodb.service";
import { DI } from "../di/di.container";


export class ShipmentsService {
    private dbService: DBService;
    constructor() {
        this.dbService = DI.get<DBService>(DBService);
    }

    async fetchIntransitShipments() {
        return new Promise(async (resolve, reject) => {
            try {

                let filterObj: any = [
                    // {
                    //     $match: {
                    //         // createdBy: { $in: ['bwl', 'realtrace', 'yusen'] }
                    //         shipmentReference: "5c362fbb-1e23-40",
                    //     }
                    // },
                    {
                        $lookup: {
                            from: 'carrierBookingLines',
                            let: {
                                docReference: '$docReference'
                            },
                            pipeline: [
                                {
                                    $match: {
                                        $expr: {
                                            $and: [
                                                {
                                                    $eq: ['$docReference', '$$docReference']
                                                },
                                                {
                                                    $not: { $in: ['$shipmentStatus', ["delivered", "gateOut"]] }
                                                },
                                                {
                                                    $not: { $in: ['$brStatus', ['open', 'rejected']] }
                                                }
                                            ]
                                        }
                                    }
                                },
                                {
                                    $limit: 1
                                }
                            ], as: 'carrierLines'
                        }
                    },
                    {
                        $unwind: '$carrierLines'
                    },
                    {
                        $lookup: {
                            from: 'carrierBookingHeader',        //for customer info to store in event db
                            let: {
                                docReference: '$carrierLines.docReference'
                            },
                            pipeline: [
                                {
                                    $match: {
                                        $expr: {
                                            $and: [
                                                {
                                                    $eq: ['$docReference', '$$docReference']
                                                }
                                            ]
                                        }
                                    }
                                },
                                {
                                    $limit: 1
                                }
                            ], as: 'carrierHeaders'
                        }
                    },
                    {
                        $unwind: '$carrierHeaders'
                    }
                ];
                let shipments: any = []
                shipments = await this.dbService.getByArrayArgToNestedArrayQuery(collectionNames.collName_shipmentRegistrations, filterObj);
                return resolve(shipments);
            } catch (err) {
                return reject(err)
            }
        })
    }
}