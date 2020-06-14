'use strict';
const mqtt = require('mqtt');
const _ = require('lodash');
const gatewaySerial = process.env.GATEWAY_SERIAL;

class Reducer {
    id;
    inputMqttClient;
    outputMqttClient;
    outputTopic;
    streamCount;
    reducables = {};
    constructor(functionInstanceId, input, output) {
        this.id = `${functionInstanceId}`; // Construct this reducer's id from function instance name and input device serial 
        input.host = input.host.startsWith('mqtt://') ? input.host : 'mqtt://' + input.host; // In case orchestrator forgot to put the protocol in hostname
        this.inputMqttClient = mqtt.connect(input.host, {
            // Carefully construct the client id
            clientId: `GATEWAY/${gatewaySerial}/FOGMR/aggregate-add/${functionInstanceId}/reducer-input-client`
        });
        this.inputMqttClient.on("connect", function () {
            console.log(`Connected to broker ${input.host} for intermediate input of function instance ${functionInstanceId}`);
        });
        this.inputMqttClient.subscribe(input.topic) // Subscribe to the input stream;
        // 'this' object inside the message callback refers to the mqtt client object, not the reducer
        // So we inject this inside the mqtt client object
        this.inputMqttClient._reducer = this;
        this.inputMqttClient.on("message", this._reduce);
        this.streamCount = input.streamCount;

        output.host = output.host.startsWith('mqtt://') ? output.host : 'mqtt://' + output.host; // In case orchestrator forgot to put the protocol in hostname
        this.outputMqttClient = mqtt.connect(output.host, {
            // Carefully construct the client id
            clientId: `GATEWAY/${gatewaySerial}/FOGMR/aggregate-add/${functionInstanceId}/reducer-output-client`
        });
        this.outputMqttClient.on("connect", function () {
            console.log(`Connected to broker ${output.host} to output reduced value of function instance ${functionInstanceId}`);
        });
        this.outputTopic = output.topic;
    }

    _reduce(topic, message) {
        message = message.toString();
        // Parse the message
        const intermediateObject = JSON.parse(message);
        let reducables = this._reducer.reducables;
        for (let key in intermediateObject) {
            reducables[key] = reducables[key] || [];
            reducables[key].push(intermediateObject[key]);
            if (reducables[key].length === this._reducer.streamCount) {
                // Time to reduce
                const reducedValue = _.reduce(reducables[key], (acc, datapoint)=>{
                    // acc is an array of reduced values upto this point
                    // convert datapoint to array
                    datapoint = datapoint.split(',');
                    // convert all to numbers
                    datapoint = datapoint.map(d=>+d);
                    // Create an array consisting of paired elements from both arrays
                    let c =_.zip(acc,datapoint);
                    // Sum the paired elements
                    c= c.map(el=>_.sum(el));
                    c[0]=0;
                    c[1]=datapoint[1];
                    return c;

                }, [])
                console.log(reducedValue)
                this._reducer._emit(reducedValue.toString());
            }
        }

    }

    _emit(value) {
        this.outputMqttClient.publish(this.outputTopic, value);
    }


}

module.exports = Reducer;