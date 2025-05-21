var _ = require('lodash');

module.exports = function (RED) {
    "use strict";
    var Influx = require('influx');
    var { InfluxDB: InfluxDBClientV2, Point: PointV2 } = require('@influxdata/influxdb-client');
    var { InfluxDBClient: InfluxDBClientV3, Point: PointV3 } = require('@influxdata/influxdb3-client');

    const VERSION_1X = '1.x';
    const VERSION_18_FLUX = '1.8-flux';
    const VERSION_20 = '2.0';
    const VERSION_30 = '3.0';

    /**
     * Config node. Currently we only connect to one host.
     */
    function InfluxConfigNode(n) {
        RED.nodes.createNode(this, n);
        this.name = n.name;             
        this.influxdbVersion = n.influxdbVersion || VERSION_1X;

        var clientOptions = null;   

        if (this.influxdbVersion === VERSION_1X) {
            this.hostname = n.hostname;
            this.port = n.port;
            this.database = n.database;            
            this.usetls = n.usetls;
            if (typeof this.usetls === 'undefined') {
                this.usetls = false;
            }
            // for backward compatibility with old 'protocol' setting
            if (n.protocol === 'https') {
                this.usetls = true;
            }
            if (this.usetls && n.tls) {
                var tlsNode = RED.nodes.getNode(n.tls);
                if (tlsNode) {
                    this.hostOptions = {};
                    tlsNode.addTLSOptions(this.hostOptions);
                }
            }
            this.client = new Influx.InfluxDB({
                hosts: [{
                    host: this.hostname,
                    port: this.port,
                    protocol: this.usetls ? "https" : "http",
                    options: this.hostOptions
                }],
                database: this.database,
                username: this.credentials.username,
                password: this.credentials.password
            });
        } else if (this.influxdbVersion === VERSION_18_FLUX || this.influxdbVersion === VERSION_20) {
            const timeout =  Math.floor(+(n.timeout?n.timeout:10)*1000) // convert from seconds to milliseconds

            const token = this.influxdbVersion === VERSION_18_FLUX ?
                `${this.credentials.username}:${this.credentials.password}` :
                this.credentials.token;

            clientOptions = {
                url: n.url,
                token,
                timeout
            }

            const v2RejectUnauthorized = n.rejectUnauthorized;

            if (v2RejectUnauthorized !== undefined) {
                clientOptions.transportOptions = {
                    rejectUnauthorized: v2RejectUnauthorized
                };
            }

            this.client = new InfluxDBClientV2(clientOptions);
        } else if (this.influxdbVersion === VERSION_30) {            
            if (!n.url) {
                this.error(RED._("influxdb.errors.missingconfig", { reason: "URL not configured for v3.0" }));
                return;
            }
            if (!this.credentials.token) {
                this.error(RED._("influxdb.errors.missingtoken"));
                return;
            }
             if (!dbForV3) {
                this.error(RED._("influxdb.errors.missingconfig", { reason: "Database not configured for v3.0" }));
                return;
            }   
            const v3Timeout = n.timeout !== undefined ? Math.floor(+(n.timeout) * 1000) : 10000;
            const v3RejectUnauthorized = n.rejectUnauthorized;

            clientOptions = {
                host: n.url, 
                token: this.credentials.token,
                timeout: v3Timeout,
            };
        
            if (v3RejectUnauthorized !== undefined) {
                clientOptions.transportOptions = {
                    rejectUnauthorized: v3RejectUnauthorized
                };
            }
            try {
                this.client = new InfluxDBClientV3(clientOptions);
                this.activeQueries = new Set();
            } catch (e) {
                this.error(RED._("influxdb.errors.invalidconfig", { reason: e.message || String(e) }));
            }            
        }
    }

    RED.nodes.registerType("influxdb", InfluxConfigNode, {
        credentials: {
            username: { type: "text" },
            password: { type: "password" },
            token: { type: "password" }
        }
    });

    function isIntegerString(value) {
        return /^-?\d+i$/.test(value);
    }

    function setFieldIntegers(fields) {
        for (const prop in fields) {
            const value = fields[prop];
            if (isIntegerString(value)) {
                fields[prop] = parseInt(value.substring(0,value.length-1));
            }
        }
    }

    function addFieldToPointV2(pointV2, name, value) {
        if (name === 'time') {
            pointV2.timestamp(value);
        } else if (typeof value === 'number') {
            pointV2.floatField(name, value);
        } else if (typeof value === 'string') {
            // string values with numbers ending with 'i' are considered integers            
            if (isIntegerString(value)) {
                value = parseInt(value.substring(0,value.length-1));
                pointV2.intField(name, value);
            } else {
                pointV2.stringField(name, value);
            }
        } else if (typeof value === 'boolean') {
            pointV2.booleanField(name, value);
        }
    }

    function addFieldToPointV3(pointV3, name, value) {
        if (name === 'time') { 
            return; 
        }
        if (typeof value === 'number') {
            if (Number.isInteger(value)) {
                pointV3.setIntegerField(name, value);
            } else {
                pointV3.setFloatField(name, value);
            }
        } else if (typeof value === 'string') {
            // string values with numbers ending with 'i' are considered integers    
            if (isIntegerString(value)) {
                pointV3.setIntegerField(name, parseInt(value.substring(0, value.length - 1)));
            } else {
                pointV3.setStringField(name, value);
            }
        } else if (typeof value === 'boolean') {
            pointV3.setBooleanField(name, value);
        } else if (typeof value === 'bigint') { 
            pointV3.setIntegerField(name, value); 
        }
        // Null or undefined values are ignored
    }

    function addFieldsToPointV2(pointV2, fields) {
        for (const prop in fields) {
            const value = fields[prop];
            addFieldToPointV2(pointV2, prop, value);
        }
    }

    function addFieldsToPointV3(pointV3, fields) {
        for (const prop in fields) {
            if (prop === 'time') continue; 
            addFieldToPointV3(pointV3, prop, fields[prop]);
        }
    }

    // Write using influx-client-js
    function writePointsV2(msg, node, done) {
        var measurement = msg.hasOwnProperty('measurement') ? msg.measurement : node.measurement;
        if (!measurement) {
            return done(RED._("influxdb.errors.nomeasurement"));
        }
        try {
            if (_.isArray(msg.payload) && msg.payload.length > 0) {
                // array of arrays: multiple points with fields and tags
                if (_.isArray(msg.payload[0]) && msg.payload[0].length > 0) {
                    msg.payload.forEach(element => {
                        let pointV2 = new PointV2(measurement);
                        let fields = element[0];
                        addFieldsToPointV2(pointV2, fields);
                        let tags = element[1];
                        for (const prop in tags) {
                            pointV2.tag(prop, tags[prop]);
                        }
                        node.client.writePoint(pointV2);
                    });
                } else {
                    // array of non-arrays: one point with both fields and tags
                    let pointV2 = new PointV2(measurement);
                    let fields = msg.payload[0];
                    addFieldsToPointV2(pointV2, fields);
                    const tags = msg.payload[1];
                    for (const prop in tags) {
                        pointV2.tag(prop, tags[prop]);
                    }
                    node.client.writePoint(pointV2)
                }
            } else {
                // single object: fields only
                if (_.isPlainObject(msg.payload)) {
                    let point = new PointV2(measurement);
                    let fields = msg.payload;
                    addFieldsToPointV2(point, fields);
                    node.client.writePoint(point);
                } else {
                    // just a value
                    let point = new PointV2(measurement);
                    let value = msg.payload;
                    addFieldToPointV2(point, 'value', value);
                    node.client.writePoint(point);
                }
            }
    
            node.client.flush(true).then(() => {
                    done();
                }).catch(error => {
                    msg.influx_error = {
                        errorMessage: error
                    };
                    done(error);
                });
        } catch (error) {
            msg.influx_error = {
                errorMessage: error
            };
            done(error);
        }
    }

    function writePointsV3(msg, node, done) { // 'node' is 'influxdb out' node
        const measurement = msg.measurement || node.measurement;
        if (!measurement) {
            node.status({fill:"red",shape:"ring",text:"influxdb.errors.nomeasurement"});
            return done(RED._("influxdb.errors.nomeasurement"));
        }        
        // For V3, the 'database' is obtained from the 'bucket' field of the 'out' node
        const db_v3 = node.bucket
        const org_v3 = node.org
        // Precision is obtained from the 'precisionV30' field of the 'out' node, or from msg.precision
        const precision_v3 = msg.precision || node.precisionV30;

        const points_v3 = [];
        try {
            if (_.isArray(msg.payload) && msg.payload.length > 0) {
                if (_.isArray(msg.payload[0]) && msg.payload[0].length > 0) { // Array of arrays [[fields,tags],[fields,tags]]
                    msg.payload.forEach(element => {
                        let point = new PointV3(measurement);
                        let fields = element[0];
                        addFieldsToPointV3(point, fields);
                        let tags = element[1];
                        if (tags && typeof tags === 'object') {
                            for (const prop in tags) {
                                point.setTag(prop, String(tags[prop]));
                            }
                        }
                        // Timestamp handling: if 'time' exists in fields, use it. Otherwise, the library will set the current one.
                        if (fields && fields.time !== undefined) {
                            point.setTimestamp(fields.time);
                        }
                        points_v3.push(point);
                    });
                } else { // Array [fields,tags]
                    let point = new PointV3(measurement);
                    let fields = msg.payload[0];
                    addFieldsToPointV3(point, fields);
                    const tags = msg.payload[1];
                    if (tags && typeof tags === 'object') {
                        for (const prop in tags) {
                            point.setTag(prop, String(tags[prop]));
                        }
                    }
                    if (fields && fields.time !== undefined) {
                        point.setTimestamp(fields.time);
                    }
                    points_v3.push(point);
                }
                } else { // Objeto od fields
                    let point = new PointV3(measurement);
                    if (_.isPlainObject(msg.payload)) {
                        let fields = msg.payload;
                        addFieldsToPointV3(point, fields);
                        if (fields && fields.time !== undefined) {
                            point.setTimestamp(fields.time);
                        }
                } else { // Single value
                    addFieldToPointV3(point, 'value', msg.payload);                    
                }
                points_v3.push(point);
            }

            if (points_v3.length === 0) {
                node.warn("No points to write for InfluxDB v3.0");
                node.status({}); // Clean status if previously was "writing"
                return done();
            }
            
            // Use the client from the config node            
            node.influxdbConfig.client.write(points_v3, db_v3, org_v3, { precision: precision_v3 })
                .then(() => {
                    node.status({}); // Clean status
                    done();
                })
                .catch(err => {
                    node.status({fill:"red",shape:"ring",text:"influxdb.status.error"});                    
                    let statusCode = err.code || err.statusCode;
                    if (err.json && err.json.code) {
                        statusCode = err.json.code;
                    }
                    msg.influx_error = { errorMessage: err.message || String(err), statusCode: statusCode };
                    done(err);
                });
        } catch (error) {
            node.status({fill:"red",shape:"ring",text:"influxdb.status.error"});
            msg.influx_error = { errorMessage: error.message || String(error) };
            done(error);
        }
    }

    /**
     * Output node to write to a single influxdb measurement
     */
    function InfluxOutNode(n) {
        RED.nodes.createNode(this, n);
        this.measurement = n.measurement;
        this.influxdb = n.influxdb;
        this.influxdbConfig = RED.nodes.getNode(this.influxdb);
        
        // Properties for 1.x
        this.precision = n.precision;
        this.retentionPolicy = n.retentionPolicy;

        // Properies for 1.8, 2.0 and 3.0
        this.database = n.database;
        this.precisionV18FluxV20 = n.precisionV18FluxV20;
        this.retentionPolicyV18Flux = n.retentionPolicyV18Flux;
        this.org = n.org;
        this.bucket = n.bucket;
        this.precisionV30 = n.precisionV30 || "ms";

        if (!this.influxdbConfig) {
            this.error(RED._("influxdb.errors.missingconfig", {reason: "InfluxDB server not configured"}));
            return;
        }
        let version = this.influxdbConfig.influxdbVersion;

        var node = this;  // Capure 'this' to be used in 'input' listener
        node.status({}); // Clear initial status

        if (version === VERSION_1X) {
            var client = this.influxdbConfig.client;

            node.on("input", function (msg, send, done) {
                var measurement;
                var writeOptions = {};

                var measurement = msg.hasOwnProperty('measurement') ? msg.measurement : node.measurement;
                if (!measurement) {
                    return done(RED._("influxdb.errors.nomeasurement"));
                }
                var precision = msg.hasOwnProperty('precision') ? msg.precision : node.precision;
                var retentionPolicy = msg.hasOwnProperty('retentionPolicy') ? msg.retentionPolicy : node.retentionPolicy;

                if (precision) {
                    writeOptions.precision = precision;
                }

                if (retentionPolicy) {
                    writeOptions.retentionPolicy = retentionPolicy;
                }

                // format payload to match new writePoints API
                var points = [];
                var point;
                if (_.isArray(msg.payload) && msg.payload.length > 0) {
                    // array of arrays
                    if (_.isArray(msg.payload[0]) && msg.payload[0].length > 0) {
                        msg.payload.forEach(function (nodeRedPoint) {
                            let fields = _.clone(nodeRedPoint[0])
                            point = {
                                measurement: measurement,
                                fields,
                                tags: nodeRedPoint[1]
                            }
                            setFieldIntegers(point.fields)
                            if (point.fields.time) {
                                point.timestamp = point.fields.time;
                                delete point.fields.time;
                            }
                            points.push(point);
                        });
                    } else {
                        // array of non-arrays, assume one point with both fields and tags
                        let fields = _.clone(msg.payload[0])
                        point = {
                            measurement: measurement,
                            fields,
                            tags: msg.payload[1]
                        };
                        setFieldIntegers(point.fields)
                        if (point.fields.time) {
                            point.timestamp = point.fields.time;
                            delete point.fields.time;
                        }
                        points.push(point);
                    }
                } else {
                    // fields only
                    if (_.isPlainObject(msg.payload)) {
                        let fields = _.clone(msg.payload)
                        point = {
                            measurement: measurement,
                            fields,
                        };
                        setFieldIntegers(point.fields)
                        if (point.fields.time) {
                            point.timestamp = point.fields.time;
                            delete point.fields.time;
                        }
                    } else {
                        // just a value
                        point = {
                            measurement: measurement,
                            fields: { value: msg.payload }
                        };
                        setFieldIntegers(point.fields)
                    }
                    points.push(point);
                }

                client.writePoints(points, writeOptions).then(() => {
                    done();
                }).catch(function (err) {
                    msg.influx_error = {
                        statusCode: err.res ? err.res.statusCode : 503
                    }
                    done(err);
                });
            });
        } else if (version === VERSION_18_FLUX || version === VERSION_20) {
            let bucket = this.bucket;
            if (version === VERSION_18_FLUX) {
                let retentionPolicy = this.retentionPolicyV18Flux ? this.retentionPolicyV18Flux : 'autogen';
                bucket = `${this.database}/${retentionPolicy}`;
            }
            let org = version === VERSION_18_FLUX ? '' : this.org;

            this.client = this.influxdbConfig.client.getWriteApi(org, bucket, this.precisionV18FluxV20);

            node.on("input", function (msg, send, done) {
                writePointsV2(msg, node, done);
            });
        } else if (version === VERSION_30) {
            node.on("input", function (msg, send, done) {
                node.status({fill:"blue",shape:"dot",text:"influxdb.status.writing"});
                writePointsV3(msg, node, done);
            });
        }
    }

    RED.nodes.registerType("influxdb out", InfluxOutNode);

    /**
     * Output node to write to multiple InfluxDb measurements
     */
    function InfluxBatchNode(n) {
        RED.nodes.createNode(this, n);
        this.influxdb = n.influxdb;
        this.influxdbConfig = RED.nodes.getNode(this.influxdb);
        
        // Properties for 1.x
        this.precision = n.precision;
        this.retentionPolicy = n.retentionPolicy;

        // Properies for 1.8, 2.0 and 3.0
        this.database = n.database;
        this.precisionV18FluxV20 = n.precisionV18FluxV20;
        this.retentionPolicyV18Flux = n.retentionPolicyV18Flux;
        this.org = n.org;
        this.bucket = n.bucket;
        this.precisionV30 = n.precisionV30 || "ms";


        if (!this.influxdbConfig) {
            this.error(RED._("influxdb.errors.missingconfig"));
            return;
        }
        let version = this.influxdbConfig.influxdbVersion;

        var node = this;

        if (version === VERSION_1X) {
            var client = this.influxdbConfig.client;

            node.on("input", function (msg, send, done) {
                var writeOptions = {};
                var precision = msg.hasOwnProperty('precision') ? msg.precision : node.precision;
                var retentionPolicy = msg.hasOwnProperty('retentionPolicy') ? msg.retentionPolicy : node.retentionPolicy;

                if (precision) {
                    writeOptions.precision = precision;
                }

                if (retentionPolicy) {
                    writeOptions.retentionPolicy = retentionPolicy;
                }

                client.writePoints(msg.payload, writeOptions).then(() => {
                    done();
                }).catch(function (err) {
                    msg.influx_error = {
                        statusCode: err.res ? err.res.statusCode : 503
                    }
                    done(err);
                });
            });
        } else if (version === VERSION_18_FLUX || version === VERSION_20) {
            let bucket = node.bucket;
            if (version === VERSION_18_FLUX) {
                let retentionPolicy = this.retentionPolicyV18Flux ? this.retentionPolicyV18Flux : 'autogen';
                bucket = `${this.database}/${retentionPolicy}`;
            }
            let org = version === VERSION_18_FLUX ? '' : this.org;

            var client = this.influxdbConfig.client.getWriteApi(org, bucket, this.precisionV18FluxV20);

            node.on("input", function (msg, send, done) {

                msg.payload.forEach(element => {
                    let pointV2 = new PointV2(element.measurement);
        
                    // time is reserved as a field name still! will be overridden by the timestamp below.
                    addFieldsToPointV2(pointV2, element.fields);

                    let tags = element.tags;
                    if (tags) {
                        for (const prop in tags) {
                            pointV2.tag(prop, tags[prop]);
                        }
                    }
                    if (element.timestamp) {
                        pointV2.timestamp(element.timestamp);
                    }
                    client.writePoint(pointV2);
                });

                // ensure we write everything including scheduled retries
                client.flush(true).then(() => {
                        done();
                    }).catch(error => {
                        msg.influx_error = {
                            errorMessage: error
                        };
                        done(error);
                    });
            });
        } else if(version === VERSION_30) {
            node.on("input", function (msg, send, done) {
                node.status({fill:"blue",shape:"dot",text:"influxdb.status.writing"});
                if (!Array.isArray(msg.payload)) {
                    node.status({fill:"red",shape:"ring",text:"influxdb.status.error"});
                    return done(new Error("msg.payload must be an array for batch node"));
                }
                const db_v3_batch = node.bucket;
                const org_v3_batch = node.org;
                const precision_v3_batch = msg.precision || node.precisionV30;

                try {
                    const points_v3_batch = msg.payload.map(element => {
                        if (!element.measurement) {
                            node.warn(`Batch point missing 'measurement', skipping: ${JSON.stringify(element)}`);
                            return null;
                        }
                        const point = new PointV3(element.measurement);
                        if (element.fields && typeof element.fields === 'object') {
                            addFieldsToPointV3(point, element.fields);
                        }
                        if (element.tags && typeof element.tags === 'object') {
                            for (const prop in element.tags) {
                                point.setTag(prop, String(element.tags[prop]));
                            }
                        }                        
                        if (element.timestamp !== undefined) {
                            point.setTimestamp(element.timestamp);
                        } else if (element.fields && element.fields.time !== undefined) {
                            point.setTimestamp(element.fields.time); // Use fields.time if timestamp is not provided                            
                        }                        
                        return point;
                    }).filter(p => p !== null); // Filtrar los puntos que eran inválidos (sin measurement)

                    if (points_v3_batch.length === 0 && msg.payload.length > 0) {
                        node.warn("All points in batch were invalid (e.g., missing measurement).");
                        node.status({});
                        return done();
                    }
                     if (points_v3_batch.length === 0) {
                         node.warn("No valid points to write in batch.");
                         node.status({});
                         return done();
                    }

                    node.influxdbConfig.client.write(points_v3_batch, db_v3_batch, org_v3_batch, { precision: precision_v3_batch })
                        .then(() => {
                            node.status({});
                            done();
                        })
                        .catch(err => {
                            node.status({fill:"red",shape:"ring",text:"influxdb.status.error"});
                            let statusCode = err.code || err.statusCode;
                            if (err.json && err.json.code) { statusCode = err.json.code; }
                            msg.influx_error = { errorMessage: err.message || String(err), statusCode: statusCode };
                            done(err);
                        });
                } catch (error) {
                    node.status({fill:"red",shape:"ring",text:"influxdb.status.error"});
                    msg.influx_error = { errorMessage: error.message || String(error) };
                    done(error);
                }
            });
        }         
    }

    RED.nodes.registerType("influxdb batch", InfluxBatchNode);

    /**
     * Input node to make queries to influxdb
     */
    function InfluxInNode(n) {
        RED.nodes.createNode(this, n);
        this.influxdb = n.influxdb;
        this.influxdbConfig = RED.nodes.getNode(this.influxdb);

        this.query = n.query;
        // Properties for InfluxDB v1.x
        this.rawOutput = n.rawOutput;
        this.precision = n.precision;
        this.retentionPolicy = n.retentionPolicy;
        // Properties for InfluxDB v2.0 and v3.0        
        this.org = n.org;
        // Specific properties for InfluxDB v3.0
        this.queryLanguage = n.queryLanguage || "influxql"; // Defaults to influxql
        this.databaseV3 = n.database; // Database for InfluxDB v3.0

        var node = this; // Capure 'this' to be used in 'input' listener

        if (!this.influxdbConfig) {
            this.error(RED._("influxdb.errors.missingconfig"));
            return;
        }

        node.status({}); // Clean initial status

        let version = this.influxdbConfig.influxdbVersion

        if (version === VERSION_1X) {            
            var client = this.influxdbConfig.client;

            node.on("input", function (msg, send, done) {
                var query;
                var rawOutput;
                var queryOptions = {};
                var precision;
                var retentionPolicy;

                query = msg.hasOwnProperty('query') ? msg.query : node.query;
                if (!query) {
                    return done(RED._("influxdb.errors.noquery"));
                }

                rawOutput = msg.hasOwnProperty('rawOutput') ? msg.rawOutput : node.rawOutput;
                precision = msg.hasOwnProperty('precision') ? msg.precision : node.precision;
                retentionPolicy = msg.hasOwnProperty('retentionPolicy') ? msg.retentionPolicy : node.retentionPolicy;

                if (precision) {
                    queryOptions.precision = precision;
                }

                if (retentionPolicy) {
                    queryOptions.retentionPolicy = retentionPolicy;
                }

                if (rawOutput) {
                    var queryPromise = client.queryRaw(query, queryOptions);
                } else {
                    var queryPromise = client.query(query, queryOptions);
                }

                queryPromise.then(function (results) {
                    msg.payload = results;
                    send(msg);
                    done();
                }).catch(function (err) {
                    msg.influx_error = {
                        statusCode: err.res ? err.res.statusCode : 503
                    }
                    done(err);
                });
            });

        } else if (version === VERSION_18_FLUX || version === VERSION_20) {
            let org = version === VERSION_20 ? this.org : ''
            this.client = this.influxdbConfig.client.getQueryApi(org);            

            node.on("input", function (msg, send, done) {
                var query = msg.hasOwnProperty('query') ? msg.query : node.query;
                if (!query) {
                    return done(RED._("influxdb.errors.noquery"));
                }
                var output = [];
                node.client.queryRows(query, {
                    next(row, tableMeta) {
                        var o = tableMeta.toObject(row)
                        output.push(o);
                    },
                    error(error) {
                        msg.influx_error = {
                            errorMessage: error
                        };
                        done(error);
                    },
                    complete() {
                        msg.payload = output;
                        send(msg);
                        done();
                    },
                });
            });
        } else if (version === VERSION_30) {
            node.on("input", function (msg, send, done) {
                node.status({fill:"blue",shape:"dot",text:"influxdb.status.querying"});
                const query_v3 = msg.query || node.query; // query from msg or node config
                if (!query_v3) {
                    node.status({fill:"red",shape:"ring",text:"influxdb.errors.noquery"});
                    return done(RED._("influxdb.errors.noquery"));
                }
                const db_v3_in = node.databaseV3;
                const queryLang_v3 = msg.queryLanguage || node.queryLanguage;
                const queryOptions_v3 = { type: queryLang_v3 };
                
                const queryId = Symbol(); // Unique identifier for this query execution
                node.influxdbConfig.activeQueries.add(queryId);

                (async () => {
                    const results_v3 = [];
                    try {
                        const queryResultStream = node.influxdbConfig.client.query(query_v3, db_v3_in, queryOptions_v3);
                        for await (const row of queryResultStream) {
                             if (!node.influxdbConfig.activeQueries.has(queryId)) {
                                // Query was cancelled (node closed/redeployed), stop processing
                                RED.log.debug(`InfluxDB In: Query ${queryId.toString()} cancelled.`);
                                break; 
                            }
                            results_v3.push(row);
                        }
                        
                        if (node.influxdbConfig.activeQueries.has(queryId)) { 
                            msg.payload = results_v3;
                            send(msg);
                            node.status({}); // Clear status
                            done();
                        } else {
                            // If the query was cancelled or the flow was stopped, do not call done() again
                            RED.log.debug(`InfluxDB In: Query ${queryId.toString()} was cancelled, not sending results.`);
                        }
                    } catch (err) {
                         if (node.influxdbConfig.activeQueries.has(queryId)) { 
                            node.status({fill:"red",shape:"ring",text:"influxdb.status.error"});
                            let statusCode = err.code || err.statusCode;
                            if (err.json && err.json.code) { statusCode = err.json.code; }
                            msg.influx_error = { errorMessage: err.message || String(err), statusCode: statusCode };
                            done(err);
                        } else {
                             RED.log.error(`InfluxDB In: Error after query ${queryId.toString()} was cancelled: ${err.message || String(err)}`);
                        }
                    } finally {
                        node.influxdbConfig.activeQueries.delete(queryId);
                    }
                })();
            });            
        }

        node.on('close', function(removed, done) {
            node.status({});
            if (node.influxdbConfig && node.influxdbConfig.influxdbVersion === VERSION_30) {
                if (node.influxdbConfig.activeQueries && node.influxdbConfig.activeQueries.size > 0) {
                    RED.log.debug(`InfluxDB In: Node closing, clearing ${node.influxdbConfig.activeQueries.size} active queries.`);
                    // When closing the node, cancel all active queries
                    // Iterate and close is safer than clear() if the set is modified concurrently (though it shouldn't be here)
                    const queriesToClear = new Set(node.influxdbConfig.activeQueries);
                    queriesToClear.forEach(qid => node.influxdbConfig.activeQueries.delete(qid));
                }
            }
            done();
        });    
    }

    RED.nodes.registerType("influxdb in", InfluxInNode);
}
