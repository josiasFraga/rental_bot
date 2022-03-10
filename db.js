const moment = require('moment-timezone');
moment.locale('pt-br');

async function connect(){
    if(global.connection && global.connection.state !== 'disconnected')
        return global.connection;

    const mysql = require("mysql2/promise");
    //const connection = await mysql.createConnection("mysql://rentalbot:zap3537shop11@localhost:3306/rentalbot");
    const connection = await mysql.createConnection("mysql://rentalbot:zap3537shop11@190.102.40.78:3306/rentalbot");
    console.log("Conectou no MySQL!");
    global.connection = connection;
    return connection;
}
//connect();

async function findClients() {
    const conn = await connect();
    const query = conn.query('SELECT * FROM clients');
    const rows = await query;
    return rows[0];
}

async function selectOrderNotStateds() {
    const conn = await connect();
    const now = moment().tz("America/Sao_Paulo").format('YYYY-MM-DD HH:mm:ss');
    const aMinuteAgo = moment(now).subtract(1, "minutes").format('YYYY-MM-DD HH:mm:ss');
    const query = conn.query('SELECT client_orders.*, clients.name, clients.api_key, clients.public_key FROM client_orders LEFT JOIN clients ON client_orders.client_id = clients.id WHERE (in_process = "N" OR last_update <= "' + aMinuteAgo + '") AND closed = "N" ');
    const rows = await query;
    return rows[0];
}

async function findOrder(order_id) {
    const conn = await connect();
    const query = conn.query('SELECT client_orders.* FROM client_orders WHERE client_orders.id=?', [order_id]);
    const rows = await query;
    return rows[0];
}

async function updateOrder(id, order) {
    const conn = await connect();
    let query = 'UPDATE client_orders SET in_process=?, status=?, last_update=?';
    let values = [order.in_process, order.status, order.last_update];

    if ( typeof order.price != 'undefined' ) {
        query += ", price=?";
        values.push(order.price);
    }

    if ( typeof order.closed != 'undefined' ) {
        query += ", closed=?";
        values.push(order.closed);
    }

    if ( typeof order.price_selled != 'undefined' ) {
        query += ", price_selled=?";
        values.push(order.price_selled);
    }

    if ( typeof order.cummulative_quote_qty != 'undefined' ) {
        query += ", cummulative_quote_qty=?";
        values.push(order.cummulative_quote_qty);
    }

    if ( typeof order.cummulative_quote_qty_selled != 'undefined' ) {
        query += ", cummulative_quote_qty_selled=?";
        values.push(order.cummulative_quote_qty_selled);
    }

    if ( typeof order.binance_return_buy != 'undefined' ) {
        query += ", binance_return_buy=?";
        values.push(order.binance_return_buy);
    }

    if ( typeof order.binance_return_sell != 'undefined' ) {
        query += ", binance_return_sell=?";
        values.push(order.binance_return_sell);
    }

    query += ' WHERE id=?';
    values.push(id);

    return await conn.query(query, values);

}

async function insertRebuy(data) {

    const conn = await connect();
    const query = 'INSERT INTO client_reoders (client_order_id, status, last_update, closed, price, cummulative_quote_qty, cummulative_quote_qty_selled, binance_return_buy, binance_return_sell) VALUES(?,?,?,?,?,?,?,?,?)';
    const values = [data.client_order_id, data.status, data.last_update, data.closed, data.price, data.cummulative_quote_qty, data.cummulative_quote_qty_selled, data.binance_return_buy, data.binance_return_sell];
    return await conn.query(query, values);

}

async function countReorders(client_order_id) {
    const conn = await connect();
    const query = conn.query('SELECT count(id) AS n_open_reorders FROM client_reoders WHERE client_reoders.client_order_id=? AND client_reoders.closed=?', [client_order_id, 'N']);
    const rows = await query;
    return rows[0];
}

async function findAllOpendedReorders(client_order_id) {
    const conn = await connect();
    const query = conn.query('SELECT * FROM client_reoders WHERE client_reoders.client_order_id=? AND client_reoders.closed=?', [client_order_id, 'N']);
    const rows = await query;
    return rows[0];
}

async function findLastOpendedReorder(client_order_id) {
    const conn = await connect();
    const query = conn.query('SELECT * FROM client_reoders WHERE client_reoders.client_order_id=? AND client_reoders.closed=? ORDER BY price DESC LIMIT 1', [client_order_id, 'N']);
    const rows = await query;
    return rows[0];
}

async function updateReorder(id, order) {
    const conn = await connect();
    let query = 'UPDATE client_reoders SET status=?, last_update=?';
    let values = [order.status, order.last_update];

    if ( typeof order.price != 'undefined' ) {
        query += ", price=?";
        values.push(order.price);
    }

    if ( typeof order.closed != 'undefined' ) {
        query += ", closed=?";
        values.push(order.closed);
    }

    if ( typeof order.price_selled != 'undefined' ) {
        query += ", price_selled=?";
        values.push(order.price_selled);
    }

    if ( typeof order.cummulative_quote_qty != 'undefined' ) {
        query += ", cummulative_quote_qty=?";
        values.push(order.cummulative_quote_qty);
    }

    if ( typeof order.cummulative_quote_qty_selled != 'undefined' ) {
        query += ", cummulative_quote_qty_selled=?";
        values.push(order.cummulative_quote_qty_selled);
    }

    if ( typeof order.binance_return_buy != 'undefined' ) {
        query += ", binance_return_buy=?";
        values.push(order.binance_return_buy);
    }

    if ( typeof order.binance_return_sell != 'undefined' ) {
        query += ", binance_return_sell=?";
        values.push(order.binance_return_sell);
    }

    query += ' WHERE id=?';
    values.push(id);

    return await conn.query(query, values);

}

module.exports = {selectOrderNotStateds, updateOrder, findOrder, insertRebuy, countReorders, findLastOpendedReorder, updateReorder, findAllOpendedReorders, findClients};
