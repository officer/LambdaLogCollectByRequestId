var zlib = require('zlib');
var async = require('async');

var AWS = require('aws-sdk');
var dynamoDB = new AWS.DynamoDB({
    apiVersion: '2012-08-10',
    maxRetries: process.env.MAX_RETRIES,
    region: 'us-east-2'
});

console.log('Loading');

var requestIdRegExp = new RegExp("[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}");

exports.handler = function (event, context, callback) {

    if (event != null) {
        console.log('event = ' + JSON.stringify(event));
    }
    else {
        console.log('No event object');

    }

    /*
        Generally Kinesis put some records in a event.
        So, we proceed these records asynchronousely.
    */
    async.map(event.Records, function (data, next) {
        var decodedData = new Buffer(data.kinesis.data, 'base64');
        zlib.gunzip(decodedData, function (err, log) {
            if (err) {
                console.log(JSON.stringify(err));
                next(err, null);
            }

            var logMessage = log.toString('utf-8');
            var logDatas = JSON.parse(logMessage);

            if (logDatas.messageType == "CONTROL_MESSAGE") {
                console.log("We will skip CONTROL_MESSAGE");
                next(null, "skip");
                return;
            }

            /*
                Generally kinesis put some logrecords in a record.
                So, we proceed these records asynchronousely.
            */
            async.each(logDatas.logEvents, function (logData, next) {
                var logId = logData.id;
                var logMessage = logData.message;

                var splitedString = logMessage.split(" ");

                if (splitedString[2]) {
                    if (requestIdRegExp.test(splitedString[2])) {
                        var requestId = splitedString[2];
                        updateDynamoDBTable(requestId, logId, next);
                    } else {
                        console.log("Request Id is not matched to Regular Expression. the request id is: " + splitedString[2]);
                        next("Request Id is not matched", null);
                    }
                } else {
                    console.log("invalid string" + logMessage);
                    next("Invalid message", null);
                }
            });

        });
    }, function (err, results) {

        if (err) { // FAILED
            callback(err, null);
        } else { // SUCCESS
            callback(null, results);
        }
    });
    
};


function updateDynamoDBTable(requestId, logId, callback) {
    // set getItem parameters.
    var getItemParameter = {
        Key: {
            "RequestId": {
               S: requestId
            }
        },
        TableName: "LambdaInvokeCountByRequestId"
    };

    // Execute GetItem.
    dynamoDB.getItem(getItemParameter, function (err, data) {
        // Error check
        if (err) {
            console.log("Failed to GetItem from DynamoDB with " + requestId);
            console.log(JSON.stringify(err));
            callback(err, null);
        } else {
            console.log("Succeeded to GetItem to DynamoDB with " + requestId);

            console.log(JSON.stringify(data));

            // Initialize log attribute
            var count = 1;
            var ids = [];

            // Check if item already exists
            if (data.Item) {
                count = parseInt(data.Item.Count) + 1;
                ids = JSON.parse(data.Item.LogIds);
                if (ids.indexOf(logId)) {
                    callback(nul, "The log was already put");
                }
            }

            ids.push(logId);

            // Set update parameters.
            var updateItemParameter = {
                Key: {
                    "RequestId": {
                        S: requestId
                    }
                },
                AttributeUpdates: {
                    "Count": {
                        Action: "ADD",
                        Value: {
                            N: count.toString()
                        }
                    },
                    "LogIds": {
                        Action: "ADD",
                        Value: {
                            SS: ids
                        }
                    }
                },

                TableName: "LambdaInvokeCountByRequestId"
            };

            console.log(JSON.stringify(updateItemParameter));

            // execute update item.
            dynamoDB.updateItem(updateItemParameter, function (err, result) {
                if (err) {
                    console.log("Failed to UpdateItem to DynamoDB with " + requestId + " and count: " + count);
                    console.log(JSON.stringify(err));
                    callback(err, null);
                } else {
                    console.log("Succeeded to UpdateItem to DynamoDB with " + requestId);
                    callback(null, data);
                }
            });
        }
    });
}