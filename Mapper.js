'use strict';
const mqtt = require('mqtt');
const gatewaySerial = process.env.GATEWAY_SERIAL;

class Mapper {
    id;
    inputMqttClient;
    outputMqttClient;
    constructor(functionInstanceId, deviceSerial, input, output) {
        this.id = `${functionInstanceId}-${deviceSerial}`; // Construct this mapper's id from function instance name and input device serial 
        input.host = input.host.startsWith('mqtt://') ? input.host : 'mqtt://'+input.host; // In case orchestrator forgot to put the protocol in hostname
        this.inputMqttClient = mqtt.connect(input.host, {
            // Carefully construct the client id
            clientId: `GATEWAY/${gatewaySerial}/FOGMR/aggregate-add/${functionInstanceId}/mapper-input/${deviceSerial}-client`
        });
        this.inputMqttClient.on("connect", function () {
            console.log(`Connected to broker ${input.host} for input ${deviceSerial} of function instance ${functionInstanceId}`);
        });
        this.inputMqttClient.subscribe(input.topic) // Subscribe to the input stream;
        // 'this' object inside the message callback refers to the mqtt client object, not the mapper
        // So we inject this inside the mqtt client object
        this.inputMqttClient._mapper =this;
        this.inputMqttClient.on("message", this._map);

        output.host = output.host.startsWith('mqtt://') ? output.host : 'mqtt://'+output.host; // In case orchestrator forgot to put the protocol in hostname
        this.outputMqttClient = mqtt.connect(output.host, {
            // Carefully construct the client id
            clientId: `GATEWAY/${gatewaySerial}/FOGMR/aggregate-add/${functionInstanceId}/mapper-output/${deviceSerial}-client`
        });
        this.outputMqttClient.on("connect", function () {
            console.log(`Connected to broker ${output.host} to output ${deviceSerial} of function instance ${functionInstanceId}`);
        });
    }

    _map(topic, message){
        message = message.toString();
        // Parse the message
        console.log(message);
    }

}

module.exports = Mapper;