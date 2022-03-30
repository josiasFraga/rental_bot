
const pid = process.pid;
const Binance = require('binance-api-node').default;
const moment = require('moment-timezone');
moment.locale('pt-br');
const apiUrl = process.env.API_URL;
const db = require('./db');
const log4js = require("log4js");

const apiKey = process.argv[2];
const apiSecret = process.argv[3];
const rowId = process.argv[4];
const logName = "row_"+rowId;

log4js.configure({
  appenders: { log_default: { type: "file", filename: "./logs/"+logName+".log" } },
  categories: { default: { appenders: ['log_default'], level: 'all' } }
});

const logger = log4js.getLogger('log_default');
/*logger.trace("Entering cheese testing");
logger.debug("Got cheese.");
logger.info("Cheese is Comté.");
logger.warn("Cheese is quite smelly.");
logger.error("Cheese is too ripe!");
logger.fatal("Cheese was breeding ground for listeria.");*/

async function _log(message, type = 'info', data = {}){
  if ( type == 'trace'){
    process.send(message);
    logger.trace(message);
  }
  else if ( type == 'info'){
    process.send(message);
    logger.info(message);
  }
  else if ( type == 'warn'){
    process.send(message);
    logger.warn(message);
  }
  else if ( type == 'error'){
    process.send(message);
    logger.error(message);
  }
  else if ( type == 'fatal'){
    process.send(message);
    logger.fatal(message);
  }
  else if ( type == 'debug'){
    process.send({'message': message, data: data});
    logger.info(message);
    logger.debug(data);
  }

  return true;
}

const client = Binance({
    apiKey: apiKey,
    apiSecret: apiSecret,
    getTime: () => Date.now(),
    httpBase: apiUrl
})

_log(`${pid} iniciou`);

async function buscaSaldosUsuario(symbol){
  
  const buyerSymbol = await getBuyerSimbol(symbol);
  const sellerSymbol = await getSellerSimbol(symbol);

  const saldo_compra = await getAmmountInWallet(buyerSymbol);
  const saldo_venda = await getAmmountInWallet(sellerSymbol);

  await _log('Saldo usuário moeda compra','debug',saldo_compra);
  await _log('Saldo usuário moeda venda','debug',saldo_venda);
}

async function getAmmountInWallet(symbol){ 
  
  const accountInfo = await client.accountInfo();

  if (accountInfo.balances) {
    balance = accountInfo.balances.filter((balance) => {
      return balance.asset == symbol;
    });

    if (balance.length == 0){
      return false;
    }

    await _log('saldo usuario', 'debug', balance);

    return balance;
  }
  return false;
}

async function checkHaveBalance(ammount_in_wallet, coin_value, quantity){
  return (ammount_in_wallet >= (coin_value*quantity));
}

async function getBuyerSimbol(symbol){
  if ( symbol.indexOf('BTC') === 0 ) {
    return symbol.replace('BTC', '');
  }
  else if ( symbol.indexOf('ETH') === 0 ) {
    return symbol.replace('ETH', '');
  }
  else if ( symbol.indexOf('USDT') === 0 ) {
    return symbol.replace('USDT', '');
  }
  else if ( symbol.indexOf('BUSD') === 0 ) {
    return symbol.replace('BUSD', '');
  }
  else if ( symbol.indexOf('SAND') === 0 ) {
    return symbol.replace('SAND', '');
  }
  else if ( symbol.indexOf('BNB') === 0 ) {
    return symbol.replace('BNB', '');
  }
  else if ( symbol.indexOf('AVAX') === 0 ) {
    return symbol.replace('AVAX', '');
  }
  else if ( symbol.indexOf('FTM') === 0 ) {
    return symbol.replace('FTM', '');
  }
  else if ( symbol.indexOf('SOL') === 0 ) {
    return symbol.replace('SOL', '');
  }
  else if ( symbol.indexOf('MATIC') === 0 ) {
    return symbol.replace('MATIC', '');
  }
  else if ( symbol.indexOf('DOT') === 0 ) {
    return symbol.replace('DOT', '');
  }
  else if ( symbol.indexOf('LINK') === 0 ) {
    return symbol.replace('LINK', '');
  }
  else if ( symbol.indexOf('ATOM') === 0 ) {
    return symbol.replace('ATOM', '');
  }
  else if ( symbol.indexOf('LUNA') === 0 ) {
    return symbol.replace('LUNA', '');
  }
  else if ( symbol.indexOf('ADA') === 0 ) {
    return symbol.replace('ADA', '');
  }
  else if ( symbol.indexOf('NEAR') === 0 ) {
    return symbol.replace('NEAR', '');
  }
  else if ( symbol.indexOf('FTT') === 0 ) {
    return symbol.replace('FTT', '');
  }
  else if ( symbol.indexOf('EGLD') === 0 ) {
    return symbol.replace('EGLD', '');
  }
  else if ( symbol.indexOf('HNT') === 0 ) {
    return symbol.replace('HNT', '');
  }
  else if ( symbol.indexOf('THETA') === 0 ) {
    return symbol.replace('THETA', '');
  }


  return false;
}

async function getSellerSimbol(symbol){
  if ( symbol.indexOf('BTC') === 0 ) {
    return "BTC";
  }
  else if ( symbol.indexOf('ETH') === 0 ) {
    return "ETH";
  }
  else if ( symbol.indexOf('USDT') === 0 ) {
    return "USDT";
  }
  else if ( symbol.indexOf('BUSD') === 0 ) {
    return "BUSD";
  }
  else if ( symbol.indexOf('SAND') === 0 ) {
    return symbol.replace('SAND', '');
  }
  else if ( symbol.indexOf('BNB') === 0 ) {
    return symbol.replace('BNB', '');
  }
  else if ( symbol.indexOf('AVAX') === 0 ) {
    return symbol.replace('AVAX', '');
  }
  else if ( symbol.indexOf('FTM') === 0 ) {
    return symbol.replace('FTM', '');
  }
  else if ( symbol.indexOf('SOL') === 0 ) {
    return symbol.replace('SOL', '');
  }
  else if ( symbol.indexOf('MATIC') === 0 ) {
    return symbol.replace('MATIC', '');
  }
  else if ( symbol.indexOf('DOT') === 0 ) {
    return symbol.replace('DOT', '');
  }
  else if ( symbol.indexOf('LINK') === 0 ) {
    return symbol.replace('LINK', '');
  }
  else if ( symbol.indexOf('ATOM') === 0 ) {
    return symbol.replace('ATOM', '');
  }
  else if ( symbol.indexOf('LUNA') === 0 ) {
    return symbol.replace('LUNA', '');
  }
  else if ( symbol.indexOf('ADA') === 0 ) {
    return symbol.replace('ADA', '');
  }
  else if ( symbol.indexOf('NEAR') === 0 ) {
    return symbol.replace('NEAR', '');
  }
  else if ( symbol.indexOf('FTT') === 0 ) {
    return symbol.replace('FTT', '');
  }
  else if ( symbol.indexOf('EGLD') === 0 ) {
    return symbol.replace('EGLD', '');
  }
  else if ( symbol.indexOf('HNT') === 0 ) {
    return symbol.replace('HNT', '');
  }
  else if ( symbol.indexOf('THETA') === 0 ) {
    return symbol.replace('THETA', '');
  }

  return false;
}

async function calculaVariacao(valor_bd, novo_valor) {
  const diff = parseFloat(novo_valor)-parseFloat(valor_bd);
  x = (parseFloat(diff)*100)/parseFloat(valor_bd);
  return parseFloat(x);
}

async function rebuy(order_id, max_rebuys, symbol, order_quanitty, symbol_price) {
  if ( parseInt(max_rebuys) == 0) {
    return true;
  }

  await _log('Máximo de recompras: ' + max_rebuys);

  const opened_rebuys_bd = await db.countReorders(order_id);
  const opened_rebuys = opened_rebuys_bd[0]['n_open_reorders'];
  await _log('Recompras abertas: '+ opened_rebuys);

  if ( opened_rebuys >= max_rebuys) {
    return true;
  }

  await _log('...Verificando saldo do usuário');  
  const buyerSymbol = await getBuyerSimbol(symbol);
  const ammountInWallet = await getAmmountInWallet(buyerSymbol);

  if ( !ammountInWallet ){
    await _log(`${pid} o usuário não possui saldo nesta moeda para recompra: ${buyerSymbol}`, 'warn');
    await new Promise(resolve => setTimeout(resolve, 2000));
    process.exit(401);
  }
  
  const ammountInWalletFree = ammountInWallet[0]['free'];
  const haveBalance = await checkHaveBalance(ammountInWalletFree, symbol_price, order_quanitty);

  if ( !haveBalance ){
    await _log(`${pid} saldo insuficiente para recompra: ${buyerSymbol} - ${ammountInWalletFree}`, 'warn');
    await new Promise(resolve => setTimeout(resolve, 2000));
    process.exit(401);
  }

  await _log('...Criando recompra na binance');  
  createOrder = await client.order({
    symbol: symbol,
    side: 'BUY',
    quantity: order_quanitty,
    type: 'MARKET',
  });

  if ( createOrder && createOrder.status == 'FILLED' ) {
    await buscaSaldosUsuario(symbol);
    const now = moment().tz("America/Sao_Paulo").format('YYYY-MM-DD HH:mm:ss');
    const dataToSave = {client_order_id: order_id, status: 'P', last_update: now, closed: 'N', price: symbol_price, cummulative_quote_qty: createOrder.cummulativeQuoteQty, cummulative_quote_qty_selled: null, binance_return_buy: JSON.stringify(createOrder), binance_return_sell: null};
    await _log('Dados para salvar rebuy', 'debug', dataToSave);
    return await db.insertRebuy(dataToSave);
  } else {
    await _log('Ordem ainda não filled', 'debug', createOrder);
  }

}

async function sell(order_id, symbol, order_quanitty, symbol_price) {
  
  //verificando se tem recomopras abertas para liquidar
  const count_opened_rebuys_bd = await db.countReorders(order_id);
  const count_opened_rebuys = count_opened_rebuys_bd[0]['n_open_reorders'];

  let createOrder = {};

  if ( count_opened_rebuys > 0 ){
    await _log('recompras encontradas, liquidando-as também');
    const opened_rebuys_bd = await db.findAllOpendedReorders(order_id);
    for( const element of opened_rebuys_bd ) {

      await _log('...Vendendo recompra na binance [' + element.id +']');
      createOrder = await client.order({
        symbol: symbol,
        side: 'SELL',
        quantity: order_quanitty,
        type: 'MARKET',
      });
      await _log('retorno venda vendendo recompra na binance [' + element.id +']', 'debug', createOrder);

      if ( createOrder && createOrder.status == 'FILLED' ) {
        await buscaSaldosUsuario(symbol);
        const now = moment().tz("America/Sao_Paulo").format('YYYY-MM-DD HH:mm:ss');
        return await db.updateReorder(element.id, {status: 'P', last_update: now, closed: 'Y', price_selled: symbol_price, cummulative_quote_qty_selled: createOrder.cummulativeQuoteQty, binance_return_sell: JSON.stringify(createOrder)});
      }
    };
  }

  await _log('...Vendendo ordem principal na binance');
  createOrder = await client.order({
    symbol: symbol,
    side: 'SELL',
    quantity: order_quanitty,
    type: 'MARKET',
  });

  if ( createOrder && createOrder.status == 'FILLED' ) {
    
    await buscaSaldosUsuario(symbol);
    const now = moment().tz("America/Sao_Paulo").format('YYYY-MM-DD HH:mm:ss');
    return await db.updateOrder(order_id, {in_process: 'N', status: 'P', last_update: now, closed: 'Y', price_selled: symbol_price, cummulative_quote_qty_selled: createOrder.cummulativeQuoteQty, binance_return_sell: JSON.stringify(createOrder)});
  }

}

async function checkRebuys(order_id, symbol, order_quanitty, symbol_price, expected_profit) {
  
  //verificando se tem recomopras abertas para verificar
  const count_opened_rebuys_bd = await db.countReorders(order_id);
  const count_opened_rebuys = count_opened_rebuys_bd[0]['n_open_reorders'];

  let createOrder = {};

  if ( count_opened_rebuys == 0 ){
    return true;
  }

  await _log('recompras encontradas, verificando-as');
  const opened_rebuys_bd = await db.findAllOpendedReorders(order_id);
  for( const element of opened_rebuys_bd ) {

    const price = element.price;
    const payed_ammout = parseFloat(price)*parseFloat(order_quanitty);
    const current_sale_value = parseFloat(symbol_price)*parseFloat(order_quanitty);
    let variacao = 0;
    if ( symbol_price != price ) {
      variacao = await calculaVariacao(payed_ammout, current_sale_value);
    } else {
      await _log('---Valor de mercado estável');
      await _log('________________________________________' );
      return false;
    }
    
    await _log('--');
    await _log('--Valor recompra na compra: ' + price);
    await _log('--Valor pago na compra: ' + payed_ammout);
    await _log('--Valor recompra novo: ' + symbol_price);
    await _log('--Valor atual de venda: ' + current_sale_value);
    await _log('--Variação da recompra: ' + parseFloat(variacao));

    if ( parseFloat(variacao) > 0 && parseFloat(variacao) >= parseFloat(expected_profit)) {
      await _log('...Vendendo recompra na binance [' + element.id +']');
      createOrder = await client.order({
        symbol: symbol,
        side: 'SELL',
        quantity: order_quanitty,
        type: 'MARKET',
      });

      if ( createOrder && createOrder.status == 'FILLED' ) {
        await buscaSaldosUsuario(symbol);
        const now = moment().tz("America/Sao_Paulo").format('YYYY-MM-DD HH:mm:ss');
        return await db.updateReorder(element.id, {status: 'P', last_update: now, closed: 'Y', price_selled: symbol_price, cummulative_quote_qty_selled: createOrder.cummulativeQuoteQty, binance_return_sell: JSON.stringify(createOrder)});
      }

    }
  };
}


processOrder = async(loop) => {
  try{
    await _log(`${pid} verificando mercado`);
    const now = moment().tz("America/Sao_Paulo").format('YYYY-MM-DD HH:mm:ss');
    const orderData = await db.findOrder(rowId);
  
    if ( !orderData || !orderData[0] || !orderData[0].id ){
      process.exit(404);
    }
    const o_id = orderData[0].id;
    const o_stauts = orderData[0].status;
    const symbol = orderData[0].symbol;
    const order_quanitty = orderData[0].quantity;
    const price = orderData[0].price;
    const max_rebuys = orderData[0].max_rebuys;
    const expected_profit = orderData[0].expected_profit;
    const fall_to_rebuy = orderData[0].fall_to_rebuy;
    let createOrder = {};
    await db.updateOrder(rowId, {in_process: 'Y', status: o_stauts, last_update: now});
  
    const symbol_price = await client.prices({ symbol: symbol });
    const buyerSymbol = await getBuyerSimbol(symbol);
    const sellerSymbol = await getSellerSimbol(symbol);
    const payed_ammout = parseFloat(price)*parseFloat(order_quanitty);
    const current_sale_value = parseFloat(symbol_price[symbol])*parseFloat(order_quanitty);

    let ammountInWallet = {};
    let variacao = 0;

    await _log('Símbolo Comprador: '+buyerSymbol);
    await _log('Símbolo Vendedor: '+sellerSymbol);

    //check if order is waiting
    if ( o_stauts == 'W' ){
      await _log('...Verificando saldo do usuário');  
      ammountInWallet = await getAmmountInWallet(buyerSymbol);
    
      if ( !ammountInWallet ){
        await db.updateOrder(rowId, {in_process: 'Y', status: 'WB', last_update: now});
        await _log(`${pid} o usuário não possui saldo nesta moeda: ${buyerSymbol}`, 'warn');
        process.exit(401);
      }
      
      const ammountInWalletFree = ammountInWallet[0]['free'];
      const haveBalance = await checkHaveBalance(ammountInWalletFree, symbol_price[symbol], order_quanitty);
    
      if ( !haveBalance ){
        await db.updateOrder(rowId, {in_process: 'Y', status: 'WB', last_update: now});
        await _log(`${pid} saldo insuficiente para recompra: ${buyerSymbol} - ${ammountInWalletFree}`, 'warn');
        process.exit(401);
      }


      await db.updateOrder(rowId, {in_process: 'Y', status: o_stauts, last_update: now});

      await _log('...Comprando ordem principal na binance');
      createOrder = await client.order({
        symbol: symbol,
        side: 'BUY',
        quantity: order_quanitty,
        type: 'MARKET',
      });

      if ( createOrder && createOrder.status == 'FILLED' ) {
        await buscaSaldosUsuario(symbol);
        await db.updateOrder(rowId, {in_process: 'Y', status: 'P', last_update: now, price: symbol_price[symbol], cummulative_quote_qty: createOrder.cummulativeQuoteQty, binance_return_buy: JSON.stringify(createOrder)});
      }

    }

    if ( symbol_price[symbol] != price ) {
      variacao = await calculaVariacao(payed_ammout, current_sale_value);
    } else {
      await _log('Valor de mercado estável');
      await _log('________________________________________' );
      return false;
    }
    
    await _log('Valor na compra: ' + price);
    await _log('Valor novo: ' + symbol_price[symbol]);
    await _log('Valor pago quantidade comprada: ' + payed_ammout);
    await _log('Valor de venta atual: ' + current_sale_value);
    await _log('Variação: ' + parseFloat(variacao));

    //valor vaixou
    if ( parseFloat(variacao) < 0 ) {
        
      if ( parseFloat(variacao) <= -(parseFloat(fall_to_rebuy)) ) {
        //procura a recompra aberta mais baixa
        const slowest_rebuy = await db.findLastOpendedReorder(o_id);
  
        //se não econtrou a recompra aberta mais baixa, faz uma recompra, se configurado
        if ( !slowest_rebuy || !slowest_rebuy[0] || !slowest_rebuy[0].id ){

          await _log('comprar mais 1: ' + symbol_price[symbol]);
          await rebuy(o_id, max_rebuys, symbol, order_quanitty, symbol_price[symbol]);

        //se econtrou uma recompra aberta
        } else {

          variacao_recompra = 0;
          const slowest_rebuy_payed_ammout = parseFloat(slowest_rebuy[0].price) * parseFloat(order_quanitty);
          const slowest_rebuy_current_sale_value = parseFloat(symbol_price[symbol]) * parseFloat(order_quanitty);

          //calcula a variação da recompra mais baixa
          variacao_recompra = await calculaVariacao(slowest_rebuy_payed_ammout, slowest_rebuy_current_sale_value);

          if ( parseFloat(variacao_recompra) <= -(parseFloat(fall_to_rebuy)) ) {
            await _log('comprar mais 2: ' + symbol_price[symbol]);
            await rebuy(o_id, max_rebuys, symbol, order_quanitty, symbol_price[symbol]);
          }

        }
      }

      await checkRebuys(o_id, symbol, order_quanitty, symbol_price[symbol], expected_profit);
    }//valor aumentou 
    else {

      if ( parseFloat(variacao) >= parseFloat(expected_profit) ) {
        await _log('vender: ' + symbol_price[symbol]);        
        await sell(o_id, symbol, order_quanitty, symbol_price[symbol]);
        process.exit(200);
      }
    }
    //await _log(ammountInWallet);
    // await _log(accountInfo);
    await _log('________________________________________' );

  }catch(error){
    if (error.code === -1121) {
      await _log(`${pid} Erro: Par inválido`, 'error');
    } else {
      await _log(`${pid} has broken! ${error.stack}`, 'error');
    }
    process.exit(0);
  }

  await new Promise(resolve => setTimeout(resolve, 2000));
  await processOrder(this);
}

startOrder = async() => {
  const now = moment().tz("America/Sao_Paulo").format('YYYY-MM-DD HH:mm:ss');
  const orderData = await db.findOrder(rowId);
  await db.updateOrder(rowId, {in_process: 'Y', status: orderData[0]['status'], last_update: now});
}

(async ()=>{
  await startOrder();
  await processOrder(this);
})();


//process.once("message",main);