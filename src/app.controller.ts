import { Router } from 'express';
import { Routes } from './interfaces/route.interface';


class AppController implements Routes {
    public path = '/appController';
    public router = Router();
    //   public appRoute:any = 

    constructor() {
        this.initializeRoutes();
    }

    private initializeRoutes() {
        // this.router.get(`${this.path}`,);
    }
}

export default AppController;
