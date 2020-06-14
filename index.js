'use strict';

require('dotenv').config()
const mqtt = require('mqtt');
const _ = require('lodash');
const gatewaySerial = process.env.GATEWAY_SERIAL;
const Mapper = require('./Mapper');
const Reducer = require('./Reducer');
let mappers = [];
let reducers = [];
const triggerClient = mqtt.connect(process.env.REMOTE_BROKER_URL || 'mqtt://localhost', {
    clientId: `GATEWAY/${gatewaySerial}/FOGMR/aggregate-add-client`,
    clean: true
});


triggerClient.on('connect', function () {
    console.log('Connected to remote broker to receive configuration commands');
    triggerClient.subscribe(`GATEWAY/${gatewaySerial}/FOGMR/aggregate-add/#`);
})

triggerClient.on("message", function (topic, message) {
    message = JSON.parse(message.toString());

    //GATEWAY/gateway_k_seil/FOGMR/aggregate-add/123/map/power_k_seil_a
    //GATEWAY/gateway_k_seil/FOGMR/aggregate-add/123/reduce
    switch (topic.split('/')[5]) {//map or reduce
        case "map": {
            console.log("Map triggered");
            const functionInstanceId = topic.split('/')[4];
            const deviceSerial = topic.split('/')[6];
            const id = `${functionInstanceId}-${deviceSerial}`; // Construct the mapper's id from function instance name and input device serial 
            const mapper = _.find(mappers, ['id', id]);
            if (mapper) {
                // If a mapper instance exists, then either we have to remove it or create a new instance
                // In either case we need to destroy the old instance
                console.log("Close connections and remove mapper instance");
                mapper.inputMqttClient.end();
                mapper.outputMqttClient.end();
                _.pull(mappers, mapper);
            }
            if (message.active) {
                console.log("Create new mapper instance");
                const mapper = new Mapper(functionInstanceId, deviceSerial, message.input, message.output)
                mappers.push(mapper);
            }
            break;
        }
        case "reduce": {
            console.log("Reduce triggered");
            const functionInstanceId = topic.split('/')[4];
            const id = `${functionInstanceId}`; // Construct the reducer's id from function instance name
            const reducer = _.find(reducers, ['id', id]);
            if(reducer){
                // If a reducer instance exists, then either we have to remove it or create a new instance
                // In either case we need to destroy the old instance
                console.log("Close connections and remove reducer instance");
                reducer.inputMqttClient.end();
                reducer.outputMqttClient.end();
                _.pull(reducers, reducer);
            }
            if (message.active) {
                console.log("Create new reducer instance");
                const reducer = new Reducer(functionInstanceId, message.input, message.output)
                reducers.push(reducer);
            }
            break;
        }
    }
})