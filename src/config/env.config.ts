import { config } from 'dotenv';
config({ path: `./env.${process.env.NODE_ENV || 'development'}.local` });

export const { NODE_ENV, PORT, DB_HOST, DB_PORT, DB_DATABASE, SECRET_KEY, LOG_FORMAT, ORIGIN, USER_NAME, PASSWORD } = process.env;
