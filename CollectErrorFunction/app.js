var zlib = require('zlib');
var async = require('async');

var AWS = require('aws-sdk');
var dynamoDB = new AWS.DynamoDB({
    apiVersion: '2012-08-10',
    maxRetries: process.env.MAX_RETRIES,
    region: 'us-east-2'
});

console.log('Loading');

const requestIdRegExp = new RegExp("[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}");

exports.handler = function (event, context) {

    if (event != null) {
        console.log('event = ' + JSON.stringify(event));
    }
    else {
        console.log('No event object');

    }

    async.map(event.Records, function (data, next) {
        var decodedData = new Buffer(data.kinesis.data, 'base64');
        zlib.gunzip(decodedData, function (err, log) {
            if (err) {
                console.log(JSON.stringify(err));
                next(err, null);
            }
            var logMessage = log.toString('utf-8');

            var splitedString = logMessage.split(" ");

            if (splitedString[2]) {
                if (requestIdRegExp.match(splitedString[2])) {
                    var requestId = splitedString[2];
                    updateDynamoDBTable(requestId, next);
                } else {
                    console.log("Request Id is not matched with Regular Expression" + splitedString[2]);
                    next("Request Id is not matched", null);
                }
            } else {
                console.log("invalid string" + logMessage);
                next("Invalid message", null);
            }


        });
    }, function (err, results) {

        if (err) {
            context.fail(err, null);
        } else {
            context.done(null, results);
        }
    });
    
};


function updateDynamoDBTable(requestId, callback) {
    var getItemParameter = {
        Key: {
            "RequestId": {
               S: requestId
            }
        },
        TableName: "LambdaInvokeCountByRequestId"
    };

    dynamoDB.getItem(getItemParameter, function (err, data) {
        if (err) {
            console.log("Failed to GetItem from DynamoDB with " + requestId);
            callback(err, null);
        } else {
            console.log("Succeeded to UpdateItem to DynamoDB with " + requestId);

            var count = 1;
            if (data.Item.Count) {
                var count = parseInt(data.Item.Count) + 1;
            }

            var updateItemParameter = {
                Key: {
                    "RequestId": {
                        S: requestId
                    },
                    "Count": {
                        N: count.toString()
                    }
                },
                TableName: "LambdaInvokeCountByRequestId"
            };

            dynamoDB.updateItem(updateItemParameter, function (err, result) {
                if (err) {
                    console.log("Failed to UpdateItem to DynamoDB with " + requestId + " and count: " + count);
                    callback(err, null);
                } else {
                    console.log("Succeeded to UpdateItem to DynamoDB with " + requestId);
                    next(null, data);
                }
            });
        }
    });
}