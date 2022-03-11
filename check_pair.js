const db = require('./db');
const Binance = require('binance-api-node').default;
const moment = require('moment-timezone');
moment.locale('pt-br');
const apiUrl = 'https://testnet.binance.vision';


(async () => {
    const pairs = await db.findPairs();
    const client = Binance({
        //apiKey: 'v4Kk62IbJQrmLNKOxljaGp43qtmRXzMOISGGitEKUX82I1RBgnUI9oCyseya4PBs',
        //apiSecret: 'yOQmPnZNJxFBOeQXpJdjQLcshJZ7k27AdBoif2UZI5KzNBFTIRLSUyCTiUj8FYmR',
        getTime: () => Date.now(),
        //httpBase: apiUrl
    })
    if ( pairs.length > 0 ){
       for ( el of pairs ) {
            const pair = el.pair;
            
            try{
                const symbol_price = await client.prices({ symbol: pair });
                console.log(pair + ' '  + symbol_price[pair]);
                const now = moment().tz("America/Sao_Paulo").format('YYYY-MM-DD HH:mm:ss');
                await db.updatePair(el.id, { valid: 'Y', checked: now });
            }catch(error){
                if (error.code === -1121) {
                    const now = moment().tz("America/Sao_Paulo").format('YYYY-MM-DD HH:mm:ss');
                    await db.updatePair(el.id, { valid: 'N', checked: now });
                    console.log(`${pair}: Par inválido`);
                } else {
                    console.log(`${pair} has broken! ${error}`);
                }
            }
            await new Promise(resolve => setTimeout(resolve, 2000));
            console.log('continuando...');

        };
    }
})()
