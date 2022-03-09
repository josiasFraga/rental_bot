
const db = require('./db');
const Binance = require('binance-api-node').default;
const apiUrl = 'https://testnet.binance.vision';


(async () => {
    const clients = await db.findClients();
    if ( clients.length > 0 ){
        clients.forEach(async (el, index) => {
            console.log(el.api_key);
            console.log(el.public_key);
            const client = Binance({
                apiKey: el.api_key,
                apiSecret: el.public_key,
                getTime: () => Date.now(),
                httpBase: apiUrl
            })
            await new Promise(resolve => setTimeout(resolve, 2000));
            const accountInfo = await client.accountInfo();
            console.log(accountInfo);
        });
    }
})()

