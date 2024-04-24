import { MongoError } from "mongodb";
import { MongoConnection } from "../../connections/database/mongo.connection";
import { DI } from "../../di/di.container";
import { Logger } from '../../utils/logger'; // Import the logger

export class DBService {
    private logger: Logger;

    constructor() {
        this.logger = DI.get(Logger);
    }

    getByQuery(collectionName: any, whereObject: any) {
        return new Promise((resolve, reject) => {

            console.log("collectionName", JSON.stringify(collectionName))
            console.log("whereObject", JSON.stringify(whereObject))

            MongoConnection.state().getDb().then(async db => {
                const collection = db.collection(collectionName);
                let queryCursor = collection.find<any>(whereObject, {});
                queryCursor.toArray().then((v: any) => {
                    // this.logger.debug("VisitByv", v)
                    return resolve(v);
                }, (error: any) => {
                    this.logger.error('Read Error Inner Retrieving', error);
                    return reject(error);
                }).catch((error: any) => {
                    this.logger.error('Read Error Outer Retrieving', error);
                    return reject(error);
                });
            }).catch((error: any) => {
                this.logger.error('Read Error Outer Retrieving', error);
                return reject(error);
            });;
        });

    }

    getByQueryFindOne(collectionName: any, whereObject: any) {
        return new Promise((resolve, reject) => {
            // Added Log Level based on ENV variable by Akhil on 16-08-2021

            console.log("collectionName", JSON.stringify(collectionName))
            console.log("whereObject", JSON.stringify(whereObject))

            MongoConnection.state().getDb().then(async db => {
                const collection = db.collection(collectionName);
                let queryCursor = collection.findOne<any>(whereObject, {});
                queryCursor.then((v: any) => {
                    // this.logger.debug("VisitByv", v)
                    return resolve(v);
                }, (error: any) => {
                    this.logger.error('Read Error Inner Retrieving', error);
                    return reject(error);
                }).catch((error: any) => {
                    this.logger.error('Read Error Outer Retrieving', error);
                    return reject(error);
                });
            }).catch((error: any) => {
                this.logger.error('Read Error Outer Retrieving', error);
                return reject(error);
            });;
        });

    }

    createIndex(collectionName: any, whereObject: any) {
        return new Promise((resolve, reject) => {
            if (process.env.LOG_LEVEL === 'DEBUG') {
                this.logger.debug("collectionName", JSON.stringify(collectionName))
                this.logger.debug("whereObject", JSON.stringify(whereObject))
            }
            MongoConnection.state().getDb().then(async db => {
                const collection = db.collection(collectionName);
                let queryCursor = collection.createIndex(whereObject)
                return resolve(queryCursor)
            })
        })
    }



    getFirstRecordBasedOnSortingOrder(collectionName: any, whereObject: any, order: any) {
        return new Promise((resolve, reject) => {
            // Added Log Level based on ENV variable by Akhil on 16-08-2021
            if (process.env.LOG_LEVEL === 'DEBUG') {
                this.logger.debug("collectionName", JSON.stringify(collectionName))
                this.logger.debug("whereObject", JSON.stringify(whereObject))
            }
            MongoConnection.state().getDb().then(async db => {
                const collection = db.collection(collectionName);
                let queryCursor = collection.find<any>(whereObject, {}).limit(1).sort({ $natural: order });
                queryCursor.toArray().then((v: any) => {
                    // this.logger.debug("VisitByv", v)
                    return resolve(v);
                }, (error: any) => {
                    this.logger.error('Read Error Inner Retrieving', error);
                    return reject(error);
                }).catch((error: any) => {
                    this.logger.error('Read Error Outer Retrieving', error);
                    return reject(error);
                });
            }).catch((error: any) => {
                this.logger.error('Read Error Outer Retrieving', error);
                return reject(error);
            });;
        });

    }

    getByNestedArrayQuery(collectionName: any, unwindArray: any, whereMatchObject: any) {
        return new Promise((resolve, reject) => {
            // Added Log Level based on ENV variable by Akhil on 16-08-2021
            if (process.env.LOG_LEVEL === 'DEBUG') {
                this.logger.debug("collectionName", JSON.stringify(collectionName))
                this.logger.debug("unwindArray", JSON.stringify(unwindArray))
                this.logger.debug("whereObject", JSON.stringify(whereMatchObject))
            }
            MongoConnection.state().getDb().then(async db => {
                const collection = db.collection(collectionName);
                let queryCursor = collection.aggregate<any>([unwindArray, whereMatchObject]);
                queryCursor.toArray().then((v: any) => {
                    // this.logger.debug("VisitByv", v)
                    return resolve(v);
                }, (error: any) => {
                    this.logger.error('Read Error Inner Retrieving', error);
                    return reject(error);
                }).catch((error: any) => {
                    this.logger.error('Read Error Outer Retrieving', error);
                    return reject(error);
                });
            }).catch((error: any) => {
                this.logger.error('Read Error Outer Retrieving', error);
                return reject(error);
            });;
        });

    }

    insertData(collectionName: any, data: any) {
        return new Promise(async (resolve, reject) => {

            this.logger.debug("collectionName:", JSON.stringify(collectionName))
            this.logger.debug("data:", JSON.stringify(data))
            try {
                const db = await MongoConnection.state().getDb();
                const collection = db.collection(collectionName);
                const result = await collection.insertOne(data);
                if (process.env.LOG_LEVEL === 'DEBUG') {
                    this.logger.debug("Inserted Successfully");
                }
                return resolve(result);
            } catch (error) {
                this.logger.error('Error In Inserting Record in DB:', error);
                this.logger.error(error);
                return reject(error);
            }
        })

    }

    insertDataWithSession(collectionName: any, data: any, session: any) {
        return new Promise(async (resolve, reject) => {

            this.logger.debug("collectionName:", JSON.stringify(collectionName))
            this.logger.debug("data:", JSON.stringify(data));
            this.logger.debug("session", JSON.stringify(session))
            try {
                const db = await MongoConnection.state().getDb();
                const collection = db.collection(collectionName);
                const result = await collection.insertOne(data, { session }); // Pass session in the options object
                if (process.env.LOG_LEVEL === 'DEBUG') {
                    this.logger.debug("Inserted data with session successfull");
                }
                return resolve(result);
            } catch (error) {
                this.logger.error('Error In Inserting data with session Record in DB:', error);
                this.logger.error(error);
                return reject(error);
            }
        })

    }

    insertMultipleData(collectionName: any, data: any) {
        return new Promise(async (resolve, reject) => {
            try {
                const db = await MongoConnection.state().getDb();
                const collection = db.collection(collectionName);
                const result = await collection.insertMany(data);
                if (process.env.LOG_LEVEL === 'DEBUG') {
                    this.logger.debug("insert");
                }
                return resolve(result);
            } catch (error) {
                this.logger.error('Error In Inserting Records in DB:', error);
                this.logger.error(error);
                return reject(error);
            }
        })

    }

    deleteData(collectionName: any, data: any) {
        return new Promise(async (resolve, reject) => {
            try {
                const db = await MongoConnection.state().getDb();
                const collection = db.collection(collectionName);
                const result = await collection.deleteMany(data);
                if (process.env.LOG_LEVEL === 'DEBUG') {
                    this.logger.debug("deleted successfully");
                }
                return resolve(result);
            } catch (error) {
                this.logger.error('Error In Deleting Records in DB:', error);
                this.logger.error(error);
                return reject(error);
            }
        })

    }


    updateData(collectionName: any, data: any, whereObject: any) {
        return new Promise(async (resolve, reject) => {
            if (process.env.LOG_LEVEL === 'DEBUG') {
                this.logger.debug("collectionName", JSON.stringify(collectionName))
                this.logger.debug("whereObject", JSON.stringify(whereObject))
                this.logger.debug("data", JSON.stringify(data))
            }
            try {
                const db = await MongoConnection.state().getDb();
                const collection = db.collection(collectionName);
                const result = await collection.updateOne(whereObject, { $set: data }); // Use $set to update the fields
                if (process.env.LOG_LEVEL === 'DEBUG') {
                    this.logger.debug("updated succesffuly");
                }
                return resolve(result);
            } catch (error) {
                this.logger.error('Error In Updating Record in DB:', error);
                this.logger.error(error);
                return reject(error);
            }
        })

    }

    updateAllDocumentsByQuery(collectionName: any, data: any, whereMatchObject: any) {
        return new Promise(async (resolve, reject) => {
            // Added Log Level based on ENV variable by Akhil on 16-08-2021
            if (process.env.LOG_LEVEL === 'DEBUG') {
                this.logger.debug("collectionName", JSON.stringify(collectionName))
                this.logger.debug("whereObject", JSON.stringify(whereMatchObject))
            }
            try {
                const db = await MongoConnection.state().getDb();
                const collection = db.collection(collectionName);
                const result = await collection.updateMany(whereMatchObject, { $set: data }); // Use $set to update the fields
                if (process.env.LOG_LEVEL === 'DEBUG') {
                    this.logger.debug('Data updated successfully');
                }
                return resolve(result);
            } catch (error) {
                this.logger.error('Error In Updating Records in DB', error);
                return reject(error);
            }


        })
    }

    updateMany(collectionName: any, data: any, whereMatchObject: any) {
        return new Promise(async (resolve, reject) => {
            // Added Log Level based on ENV variable by Akhil on 16-08-2021
            if (process.env.LOG_LEVEL === 'DEBUG') {
                this.logger.debug("collectionName", JSON.stringify(collectionName))
                this.logger.debug("whereObject", JSON.stringify(whereMatchObject))
            }
            try {
                const db = await MongoConnection.state().getDb();
                const collection = db.collection(collectionName);
                const result = await collection.updateMany(whereMatchObject, data); // Use $set to update the fields
                if (process.env.LOG_LEVEL === 'DEBUG') {
                    this.logger.debug('Data updated successfully');
                }
                return resolve(result);
            } catch (error) {
                this.logger.error('Error In Updating Records in DB', error);
                return reject(error);
            }


        })
    }



    getByArrayArgToNestedArrayQuery(collectionName: any, query: any) {
        return new Promise((resolve, reject) => {
            this.logger.debug("collectionName", JSON.stringify(collectionName))
            this.logger.debug("whereObject", JSON.stringify(query))
            MongoConnection.state().getDb().then(async db => {
                const collection = db.collection(collectionName);
                let queryCursor = collection.aggregate<any>(query);
                queryCursor.toArray().then((v: any) => {
                    // this.logger.debug("VisitByv", v)
                    return resolve(v);
                }, (error: any) => {
                    this.logger.error('Read Error Inner Retrieving', error);
                    return reject(error);
                }).catch((error: any) => {
                    this.logger.error('Read Error Outer Retrieving', error);
                    return reject(error);
                });
            }).catch((error: any) => {
                this.logger.error('Read Error Outer Retrieving', error);
                return reject(error);
            });;
        });

    }

    getByQueryAndSortAlphaNumeric(collectionName: any, whereObject: any, sortCondition: any) {
        return new Promise((resolve, reject) => {
            // Added Log Level based on ENV variable by Akhil on 16-08-2021
            if (process.env.LOG_LEVEL === 'DEBUG') {
                this.logger.debug("collectionName", JSON.stringify(collectionName))
                this.logger.debug("whereObject", JSON.stringify(whereObject))
            }
            MongoConnection.state().getDb().then(async db => {
                const collection = db.collection(collectionName);
                let queryCursor = collection.find<any>(whereObject, {}).sort(sortCondition).collation({ locale: "en_US", numericOrdering: true }).limit(1);
                queryCursor.toArray().then((v: any) => {
                    // this.logger.debug("VisitByv", v)
                    return resolve(v);
                }, (error: any) => {
                    this.logger.error('Read Error Inner Retrieving', error);
                    return reject(error);
                }).catch((error: any) => {
                    this.logger.error('Read Error Outer Retrieving', error);
                    return reject(error);
                });
            }).catch((error: any) => {
                this.logger.error('Read Error Outer Retrieving', error);
                return reject(error);
            });;
        });

    }

}




