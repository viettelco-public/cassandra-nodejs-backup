import cassandra from "cassandra-driver";
import moment from "moment";
import fs from "fs";
import dotenv from "dotenv";

dotenv.config()


const hidePassword = (password) => {
    const pwds = password.split("");
    return (pwds.splice(0, 3).join("") + pwds.splice(3, pwds.length - 6).map(x => "*").join("") + pwds.splice(pwds.length - 3, pwds.length - 1).join(""));
}

const HOST = process.env.HOST || '127.0.0.1';
const PORT = process.env.PORT || 9042;
const KEYSPACE = process.env.KEYSPACE;


if (!KEYSPACE) {
    console.log('`KEYSPACE` must be specified as environment variable');
    process.exit();
}

const USER = process.env.USER;
const PASSWORD = process.env.PASSWORD;
const DATACENTER = process.env.DATACENTER || "datacenter1";
const USE_SSL = process.env.USE_SSL;
const DIRECTORY = process.env.DIRECTORY || `./data/${HOST}/${DATACENTER}/${KEYSPACE}/${moment().format("YYYYMMDD-HHmmSSS")}`;
let authProvider;
if (USER && PASSWORD) {
    authProvider = new cassandra.auth.PlainTextAuthProvider(USER, PASSWORD);
}

let sslOptions;
if (USE_SSL) {
    sslOptions = {rejectUnauthorized: false};
}

export const systemClient = new cassandra.Client({
    contactPoints: [HOST],
    authProvider: authProvider,
    localDataCenter: DATACENTER,
    protocolOptions: {port: [PORT]}
});
export const client = new cassandra.Client({
    contactPoints: [HOST],
    keyspace: KEYSPACE,
    localDataCenter: DATACENTER,
    authProvider: authProvider,
    protocolOptions: {port: [PORT]}
});

export {
    HOST, PORT, USER, PASSWORD, DIRECTORY, DATACENTER,
    KEYSPACE,
}

console.table({
    Host: HOST,
    Port: PORT,
    User: USER,
    Password: hidePassword(PASSWORD),
    KeySpace: KEYSPACE,
    localDataCenter: DATACENTER,
    Directory: DIRECTORY,
});


if (!fs.existsSync(DIRECTORY)) {
    fs.mkdirSync(DIRECTORY, {recursive: true})
}
