import { NextFunction, Request, Response } from 'express';
import { HttpException } from '../exceptions/httpException';
import { Logger } from '../utils/logger';
import { DI } from "../di/di.container";


let logger: Logger = DI.get<Logger>(Logger);
const errorMiddleware = (error: HttpException, req: Request, res: Response, next: NextFunction) => {
    try {
        const status: number = error.status || 500;
        const message: string = error.message || 'Something went wrong';

        logger.error(`[${req.method}] ${req.path} >> StatusCode:: ${status}, Message:: ${message}`);
        res.status(status).json({ errorStatus: true, message });
    } catch (error) {
        next(error);
    }   
};

export default errorMiddleware;
