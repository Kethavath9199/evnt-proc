import { Router } from 'express';
import { Routes } from './interfaces/route.interface';
import AppController from './app.controller';


class AppRoute implements Routes {
    public path = '/realTraceAsync';
    public router = Router();
    //   public appRoute:any = 

    constructor() {
        this.initializeRoutes();
    }

    private initializeRoutes() {
        this.router.use(`${this.path}`, new AppController().router);
    }
}

export default AppRoute;
