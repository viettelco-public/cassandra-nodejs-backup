require('dotenv').config()
const moment = require('moment');




var HOST = process.env.HOST || '127.0.0.1';
var PORT = process.env.PORT || 9042;
var KEYSPACE = process.env.KEYSPACE;

if (!KEYSPACE) {
    console.log('`KEYSPACE` must be specified as environment variable');
    process.exit();
}

var USER = process.env.USER;
var PASSWORD = process.env.PASSWORD;
var DATACENTER = process.env.DATACENTER || "datacenter1";
var USE_SSL = process.env.USE_SSL;
var DIRECTORY = process.env.DIRECTORY || `./data/${HOST}/${DATACENTER}/${KEYSPACE}/${moment().format("YYYYMMDD-HHmmSSS")}`;

console.table({
    Host: HOST,
    Port: PORT,
    User: USER,
    Password: hidePassword(PASSWORD),
    KeySpace: KEYSPACE,
    localDataCenter: DATACENTER,
    Directory: DIRECTORY,
});