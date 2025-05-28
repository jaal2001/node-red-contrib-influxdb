var _ = require('lodash');

module.exports = function (RED) {
    "use strict";
    var { InfluxDBClient, Point } = require('@influxdata/influxdb3-client');

    /**
     * Config node for InfluxDB 3.0
     */
    function InfluxConfigNode(n) {
        RED.nodes.createNode(this, n);
        this.name = n.name;
        this.host = n.host;
        this.database = n.database;
        this.timeout = n.timeout || 10;

        // Create client options
        const clientOptions = {
            host: this.host,
            token: this.credentials.token,
            database: this.database,
            timeout: Math.floor(this.timeout * 1000) // convert seconds to milliseconds
        };

        try {
            this.client = new InfluxDBClient(clientOptions);
        } catch (error) {
            this.error("Failed to create InfluxDB client: " + error.message);
        }

        // Clean up client when node is closed
        this.on('close', function() {
            if (this.client) {
                this.client.close();
            }
        });
    }

    RED.nodes.registerType("influxdb", InfluxConfigNode, {
        credentials: {
            token: { type: "password" }
        }
    });

    /**
     * Helper function to create Point from various data formats
     */
    function createPointFromData(measurement, data) {
        const point = new Point(measurement);
        
        if (_.isPlainObject(data)) {
            // Handle object with fields and optional tags
            for (const [key, value] of Object.entries(data)) {
                if (key === 'time') {
                    point.timestamp(value);
                } else if (key === 'tags' && _.isPlainObject(value)) {
                    // Handle tags separately
                    for (const [tagKey, tagValue] of Object.entries(value)) {
                        point.tag(tagKey, String(tagValue));
                    }
                } else {
                    // Add as field
                    addFieldToPoint(point, key, value);
                }
            }
        } else {
            // Simple value
            addFieldToPoint(point, 'value', data);
        }
        
        return point;
    }

    /**
     * Helper function to add field to point with proper type handling
     */
    function addFieldToPoint(point, name, value) {
        if (typeof value === 'number') {
            if (Number.isInteger(value)) {
                point.intField(name, value);
            } else {
                point.floatField(name, value);
            }
        } else if (typeof value === 'string') {
            // Check if string represents an integer (ending with 'i')
            if (/^-?\d+i$/.test(value)) {
                const intValue = parseInt(value.substring(0, value.length - 1));
                point.intField(name, intValue);
            } else {
                point.stringField(name, value);
            }
        } else if (typeof value === 'boolean') {
            point.booleanField(name, value);
        } else {
            // Convert other types to string
            point.stringField(name, String(value));
        }
    }

    /**
     * Output node to write to InfluxDB 3.0
     */
    function InfluxOutNode(n) {
        RED.nodes.createNode(this, n);
        this.measurement = n.measurement;
        this.influxdb = n.influxdb;
        this.influxdbConfig = RED.nodes.getNode(this.influxdb);

        if (!this.influxdbConfig) {
            this.error(RED._("influxdb.errors.missingconfig"));
            return;
        }

        if (!this.influxdbConfig.client) {
            this.error("InfluxDB client not available");
            return;
        }

        var node = this;

        node.on("input", async function (msg, send, done) {
            try {
                const measurement = msg.hasOwnProperty('measurement') ? msg.measurement : node.measurement;
                if (!measurement) {
                    return done(RED._("influxdb.errors.nomeasurement"));
                }

                const database = msg.hasOwnProperty('database') ? msg.database : node.influxdbConfig.database;
                let points = [];

                if (_.isArray(msg.payload)) {
                    // Handle array of data points
                    if (msg.payload.length > 0 && _.isArray(msg.payload[0])) {
                        // Array of [fields, tags] arrays
                        msg.payload.forEach(element => {
                            const point = new Point(measurement);
                            
                            // Add fields
                            if (element[0] && _.isPlainObject(element[0])) {
                                for (const [key, value] of Object.entries(element[0])) {
                                    if (key === 'time') {
                                        point.timestamp(value);
                                    } else {
                                        addFieldToPoint(point, key, value);
                                    }
                                }
                            }
                            
                            // Add tags
                            if (element[1] && _.isPlainObject(element[1])) {
                                for (const [key, value] of Object.entries(element[1])) {
                                    point.tag(key, String(value));
                                }
                            }
                            
                            points.push(point);
                        });
                    } else {
                        // Single point with [fields, tags]
                        const point = new Point(measurement);
                        
                        // Add fields
                        if (msg.payload[0] && _.isPlainObject(msg.payload[0])) {
                            for (const [key, value] of Object.entries(msg.payload[0])) {
                                if (key === 'time') {
                                    point.timestamp(value);
                                } else {
                                    addFieldToPoint(point, key, value);
                                }
                            }
                        }
                        
                        // Add tags
                        if (msg.payload[1] && _.isPlainObject(msg.payload[1])) {
                            for (const [key, value] of Object.entries(msg.payload[1])) {
                                point.tag(key, String(value));
                            }
                        }
                        
                        points.push(point);
                    }
                } else {
                    // Single data point
                    const point = createPointFromData(measurement, msg.payload);
                    points.push(point);
                }

                // Write points to InfluxDB
                for (const point of points) {
                    await node.influxdbConfig.client.write(point.toLineProtocol(), database);
                }

                done();
            } catch (error) {
                msg.influx_error = {
                    errorMessage: error.message
                };
                done(error);
            }
        });
    }

    RED.nodes.registerType("influxdb out", InfluxOutNode);

    /**
     * Batch output node for multiple measurements
     */
    function InfluxBatchNode(n) {
        RED.nodes.createNode(this, n);
        this.influxdb = n.influxdb;
        this.influxdbConfig = RED.nodes.getNode(this.influxdb);

        if (!this.influxdbConfig) {
            this.error(RED._("influxdb.errors.missingconfig"));
            return;
        }

        if (!this.influxdbConfig.client) {
            this.error("InfluxDB client not available");
            return;
        }

        var node = this;

        node.on("input", async function (msg, send, done) {
            try {
                const database = msg.hasOwnProperty('database') ? msg.database : node.influxdbConfig.database;
                
                if (!_.isArray(msg.payload)) {
                    return done(new Error("Payload must be an array for batch operations"));
                }

                // Process each item in the batch
                for (const item of msg.payload) {
                    if (!item.measurement) {
                        continue; // Skip items without measurement
                    }

                    const point = new Point(item.measurement);

                    // Add fields
                    if (item.fields && _.isPlainObject(item.fields)) {
                        for (const [key, value] of Object.entries(item.fields)) {
                            addFieldToPoint(point, key, value);
                        }
                    }

                    // Add tags
                    if (item.tags && _.isPlainObject(item.tags)) {
                        for (const [key, value] of Object.entries(item.tags)) {
                            point.tag(key, String(value));
                        }
                    }

                    // Add timestamp if provided
                    if (item.timestamp) {
                        point.timestamp(item.timestamp);
                    }

                    // Write the point
                    await node.influxdbConfig.client.write(point.toLineProtocol(), database);
                }

                done();
            } catch (error) {
                msg.influx_error = {
                    errorMessage: error.message
                };
                done(error);
            }
        });
    }

    RED.nodes.registerType("influxdb batch", InfluxBatchNode);

    /**
     * Input node to query InfluxDB 3.0
     */
    function InfluxInNode(n) {
        RED.nodes.createNode(this, n);
        this.influxdb = n.influxdb;
        this.query = n.query;
        this.queryType = n.queryType || 'sql'; // 'sql' or 'influxql'
        this.influxdbConfig = RED.nodes.getNode(this.influxdb);

        if (!this.influxdbConfig) {
            this.error(RED._("influxdb.errors.missingconfig"));
            return;
        }

        if (!this.influxdbConfig.client) {
            this.error("InfluxDB client not available");
            return;
        }

        var node = this;

        node.on("input", async function (msg, send, done) {
            try {
                const query = msg.hasOwnProperty('query') ? msg.query : node.query;
                if (!query) {
                    return done(RED._("influxdb.errors.noquery"));
                }

                const database = msg.hasOwnProperty('database') ? msg.database : node.influxdbConfig.database;
                const queryType = msg.hasOwnProperty('queryType') ? msg.queryType : node.queryType;
                
                const queryOptions = {};
                if (queryType === 'influxql') {
                    queryOptions.type = 'influxql';
                }

                // Execute query
                const queryResult = await node.influxdbConfig.client.query(query, database, queryOptions);
                
                // Collect results
                const results = [];
                for await (const row of queryResult) {
                    results.push(row);
                }

                msg.payload = results;
                send(msg);
                done();
            } catch (error) {
                msg.influx_error = {
                    errorMessage: error.message
                };
                done(error);
            }
        });
    }

    RED.nodes.registerType("influxdb in", InfluxInNode);
}
