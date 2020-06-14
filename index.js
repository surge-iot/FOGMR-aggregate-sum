'use strict';

require('dotenv').config()
const mqtt = require('mqtt');
const _ = require('lodash');
const gatewaySerial = process.env.GATEWAY_SERIAL;
const Mapper = require('./Mapper');
let mappers = [];
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
    switch (topic.split('/')[5]) {//map or reduce
        case "map": {
            console.log("Map triggered");
            const functionInstanceId = topic.split('/')[4];
            const deviceSerial = topic.split('/')[6];
            if (message.active) {
                const mapper = new Mapper(functionInstanceId, deviceSerial, message.input, message.output)
                mappers.push(mapper);
            }
            else {
                console.log("Close connections and remove mapper instance");
                const id = `${functionInstanceId}-${deviceSerial}`; // Construct the mapper's id from function instance name and input device serial 
                const mapper = _.find(mappers, ['id', id]);
                if (!mapper) {
                    return;
                }
                mapper.inputMqttClient.end();
                mapper.outputMqttClient.end();
                _.pull(mappers, mapper);

            }
            break;
        }
        case "reduce": {
            console.log("Reduce triggered");
            break;
        }
    }
})