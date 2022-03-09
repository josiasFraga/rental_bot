
const db = require('./db');
const cp = require('child_process');
const modulePath = `${__dirname}/worker.js`;
const interval = process.env.CROWLER_INTERVAL;


setInterval(async ()=>{
    const orders = await db.selectOrderNotStateds();
    if ( orders.length > 0 ){
        orders.forEach((el, index) => {
            console.log(new Date(), 'Iniciando worker - ' + el.name + ' - ' + el.id);
            //Create new worker
            const worker = cp.fork(modulePath, [el.api_key, el.public_key, el.id]);
            worker.on("message", (msg) => {
                console.log(msg);
            });
            worker.on("close", function (code) {
                console.log("child process exited with code " + code);
            });
            worker.on("error", (msg) => {
                worker.kill('SIGHUP');
            });
            //worker.send(el);
        });
    }
},interval);

