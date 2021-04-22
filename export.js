import {client, DIRECTORY, KEYSPACE, systemClient} from "./header.js"
import fs from "fs";
import Promise from "bluebird";
import jsonStream from "JSONStream";


function processTableExport(table) {
    console.log('==================================================');
    console.log('Reading table: ' + table);
    return new Promise(function (resolve, reject) {
        const jsonfile = fs.createWriteStream(DIRECTORY + "/" + table + '.json');
        jsonfile.on('error', function (err) {
            reject(err);
        });

        let processed = 0;
        const startTime = Date.now();
        jsonfile.on('finish', function () {
            const timeTaken = (Date.now() - startTime) / 1000;
            const throughput = timeTaken ? processed / timeTaken : 0.00;
            console.log('Done with table, throughput: ' + throughput.toFixed(1) + ' rows/s');
            resolve();
        });
        const writeStream = jsonStream.stringify('[', ',', ']');
        writeStream.pipe(jsonfile);

        const query = 'SELECT * FROM "' + table + '"';
        const options = {prepare: true, fetchSize: 1000};

        client.eachRow(query, [], options, function (n, row) {
            const rowObject = {};
            row.forEach(function (value, key) {
                if (typeof value === 'number') {
                    if (Number.isNaN(value)) {
                        rowObject[key] = {
                            type: "NOT_A_NUMBER"
                        }
                    } else if (Number.isFinite(value)) {
                        rowObject[key] = value;
                    } else if (value > 0) {
                        rowObject[key] = {
                            type: "POSITIVE_INFINITY"
                        }
                    } else {
                        rowObject[key] = {
                            type: "NEGATIVE_INFINITY"
                        }
                    }
                } else {
                    rowObject[key] = value;
                }
            });

            processed++;
            writeStream.write(rowObject);
        }, function (err, result) {

            if (err) {
                reject(err);
                return;
            }

            console.log('Streaming ' + processed + ' rows to: ' + jsonfile.path);

            if (result.nextPage) {
                result.nextPage();
                return;
            }

            console.log('Finalizing writes into: ' + jsonfile.path);
            writeStream.end();
        });
    });
}

systemClient.connect()
    .then(function () {
        let systemQuery = "SELECT columnfamily_name as table_name FROM system.schema_columnfamilies WHERE keyspace_name = ?";
        if (systemClient.metadata.keyspaces.system_schema) {
            systemQuery = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?";
        }

        console.log('Finding tables in keyspace: ' + KEYSPACE);
        return systemClient.execute(systemQuery, [KEYSPACE]);
    })
    .then(function (result) {
        const tables = [];
        for (let i = 0; i < result.rows.length; i++) {
            tables.push(result.rows[i].table_name);
        }

        if (process.env.TABLE) {
            return processTableExport(process.env.TABLE);
        }

        return Promise.each(tables, function (table) {
            return processTableExport(table);
        });
    })
    .then(function () {
        console.log('==================================================');
        console.log('Completed exporting all tables from keyspace: ' + KEYSPACE);
        const gracefulShutdown = [];
        gracefulShutdown.push(systemClient.shutdown());
        gracefulShutdown.push(client.shutdown());
        Promise.all(gracefulShutdown)
            .then(function () {
                process.exit();
            })
            .catch(function (err) {
                console.log(err);
                process.exit(1);
            });
    })
    .catch(function (err) {
        console.log(err);
    });
