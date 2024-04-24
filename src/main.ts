import App from './app';
import AppRoute from './app.route';


// validateEnv();
const routesArray = [
    new AppRoute()
];

const app = new App(routesArray);

app.listen();
