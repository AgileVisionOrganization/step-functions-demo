'use strict';

const AWS = require('aws-sdk');
const async = require('async');

/**
 * Lambda function that triggers Step Function execution on an S3 event.
 * Input event is transformed and passed as a parameter to the step function.
 * Step function input has the following format:
 * {
 *    "bucketName": "<name of the S3 bucket>"
 *    "objectKey": "<key of the S3 object>",
 * } 
 * process.env.STEP_FUNCTION_NAME should contain the name of the target bucket
 */
module.exports.executeWorkflow = function (event, context) {
  if ('Records' in event) {
    const stateMachineName = process.env.STEP_FUNCTION_NAME;
    const stepfunctions = new AWS.StepFunctions();

    async.waterfall([
      (next) => {
        console.log('Fetching the list of available workflows');
        return stepfunctions.listStateMachines({}, next);
      },
      (data, next) => {
        console.log(data, next);
        console.log('Searching for the step function', data);

        for (var i = 0; i < data.stateMachines.length; i++) {
          const item = data.stateMachines[i];
          if (item.name === stateMachineName) {
            console.log('Found the step function', item);
            return next(null, item.stateMachineArn);
          }
        }

        throw 'Step function with the given name doesn\'t exist';
      },
      (stateMachineArn, next) => {
        console.log('Executing the step function', stateMachineArn);
        const eventData = event.Records[0];
        return stepfunctions.startExecution({
          stateMachineArn: stateMachineArn,
          input: JSON.stringify({ objectKey: eventData.s3.object.key, bucketName: eventData.s3.bucket.name })
        }, next);
      },
      () => {
        return context.succeed('OK');
      }
    ]);
  } else {
    return context.fail('Incoming message doesn\'t contain "Records", it will be ignored', event);
  }
};


/**
  * Lambda function that parses a CSV file, uploaded to S3 and
  * writes it's contents to the DynamoDB.
  *
  * Function assumes event contains information about uploaded file:
  * {
  *  'bucketName': '<bucket-name>',
  *  'objectKey': '<object-key>'
  * }
  */
module.exports.processFile = (event, context, callback) => {
  const csv = require('fast-csv');
  const s3 = new AWS.S3();
  const dynamodb = new AWS.DynamoDB();

  async.waterfall([
    (next) => {
      console.log('Waiting until the uploaded object becomes available',
        '[bucket = ', event.bucketName, ', key = ',
        event.objectKey, '  ]');
      s3.waitFor('objectExists', {
        Bucket: event.bucketName,
        Key: event.objectKey
      }, next);
    },
    (result, next) => {
      console.log('Downloading the CSV file from S3 [bucket = ',
        event.bucketName, ', key = ', event.objectKey, '  ]');

      const csvStream = s3.getObject({
        Bucket: event.bucketName,
        Key: event.objectKey
      }).createReadStream();

      csv.fromStream(csvStream).on('data', (data) => {
        dynamodb.putItem({
          Item: {
            'sensor_id': {
              'S': data[0]
            },
            'timestamp': {
              'N': data[1]
            },
            'value': {
              'N': data[2]
            }
          },
          TableName: "sensor_data"
        });
      });

      next(null);
    },
  ], (err, results) => {
    if (err) {
      console.log('Failed execution');
      return context.fail('Execution failed');
    } else {
      console.log('Successful execution');
      return context.succeed(event);
    }
  });
};

/**
 * Lambda function that moves the processed file to the 'processed' folder
 *
 * Function assumes event contains information about uploaded file:
 * {
 *  'bucketName': '<bucket-name>',
 *  'objectKey': '<object-key>'
 * }
 * 
 * ENV.TARGET_BUCKET should contain the name of the target bucket
 */
module.exports.moveFile = function (event, context) {
  const objectKey = event.objectKey;
  const bucketName = event.bucketName;
  const newLocation = 'processed/' + objectKey;
  const targetBucket = process.env.TARGET_BUCKET;
  const s3 = new AWS.S3();

  console.log('Moving "', objectKey, '" to new location "', newLocation, '"');
  async.waterfall([
    (next) => {
      s3.copyObject({
        Bucket: targetBucket,
        Key: newLocation,
        CopySource: bucketName + '/' + encodeURIComponent(objectKey)
      }, next);
    },
    (data, next) => {
      s3.waitFor('objectExists', {
        Bucket: targetBucket,
        Key: newLocation
      }, next);
    },
    (data, next) => {
      s3.deleteObject({
        Bucket: bucketName,
        Key: objectKey
      }, next);
    }
  ], (error) => {
    if (error) {
      console.log('Failed to move file', error);
      context.fail();
    } else {
      context.succeed({
        bucketName: event.bucketName,
        objectKey: event.objectKey,
        newLocation: newLocation
      });
    }
  });
};

/**
 * Lambda function that sends an email after processing finished
 *
 * Function assumes event contains information about uploaded file:
 * {
 *  'bucketName': '<bucket-name>',
 *  'objectKey': '<object-key>'
 * }
 * 
 */
module.exports.sendEmail = function (event, context) {
  const objectKey = event.objectKey;
  const bucketName = event.sourceBucket;
  const ses = new AWS.SES();

  console.log('Sending an email about "', objectKey, '"');
  async.waterfall([
    (next) => {
      ses.sendEmail({
        Destination: {
          ToAddresses: [process.env.DEST_EMAIL]
        },
        Message: {
          Body: {
            Text: {
              Data: 'Processed file ' + objectKey
            }
          },
          Subject: {
            Data: 'File processed'
          }
        },
        Source: process.env.DEST_EMAIL
      }, next);
    }], (err, results) => {
      if (err) {
        console.log('Failed to send an email', err);
        context.fail();
      } else {
        context.succeed("OK");
      }
    });
};
