import { MongoClient, MongoError, Db, ClientSession } from 'mongodb';
import { Logger } from '../../utils/logger';
import { DI } from '../../di/di.container';

export class MongoConnection {

    private static instance?: MongoConnection = undefined;

    private client?: MongoClient = undefined;

    private db?: Db = undefined;
    private session: any;

    public connect(): Promise<MongoClient> {
        return new Promise<MongoClient>((resolve, reject) => {
            MongoClient.connect(`mongodb://${process.env.MONGO_HOST}:${process.env.MONGO_PORT}`, { auth: { username: process.env.MONGO_USER!, password: process.env.MONGO_PASS! } }).then((res) => {
                console.debug('mongo connected succesfully...')
                resolve(res);
            }).catch((err) => {
                console.debug(`error in connecting to database Error:${err}`)
                reject()
            })
        });
    }

    static state(): MongoConnection {
        if (MongoConnection.instance === undefined) {
            MongoConnection.instance = new MongoConnection();
        }
        return MongoConnection.instance;
    }

    public getClient(): Promise<MongoClient> {
        return new Promise<MongoClient>((resolve, reject) => {
            if (this.client === undefined) {
                this.connect().then(client => {
                    this.client = client;
                    resolve(this.client);
                }).catch(error => {
                    reject(error);
                })
            } else {
                resolve(this.client);
            }
        });
    }

    getDb(): Promise<Db> {
        return new Promise<Db>((resolve, reject) => {
            if (this.db === undefined) {
                this.getClient().then(client => {
                    this.db = client.db(process.env.MONGO_DBNM);

                    // TEST
                    //  this.db = client.db("rw_uat_rr_dlr_1_test");

                    // PFI
                    // this.db = client.db("pfi_dev");


                    //Clarios
                    //   this.db = client.db("rw_uat_rr_dlr_1");

                    //Demo
                    //   this.db = client.db("config_dev");

                    //  Sresta- Prod
                    //   this.db = client.db("rw_uat_rr_dlr_1");

                    resolve(this.db);
                }).catch(error => {
                    reject(error);
                })
            } else {
                resolve(this.db);
            }
        });
    }

    getClientSession(): Promise<any> {
        return new Promise<ClientSession>((resolve, reject) => {
            if (this.session === undefined) {
                this.getClient().then(client => {
                    let session: ClientSession = client.startSession()
                    // TEST
                    //  this.db = client.db("rw_uat_rr_dlr_1_test");

                    // PFI
                    // this.db = client.db("pfi_dev");


                    //Clarios
                    //   this.db = client.db("rw_uat_rr_dlr_1");

                    //Demo
                    //   this.db = client.db("config_dev");

                    //  Sresta- Prod
                    //   this.db = client.db("rw_uat_rr_dlr_1");

                    resolve(session);
                }).catch(error => {
                    reject(error);
                })
            } else {
                resolve(this.session);
            }
        });
    }
}
