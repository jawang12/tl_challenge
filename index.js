const csv = './test.csv';
const fs = require('fs');
//Fast csv parser for node. Based on Papa parse which runs in browser.
const Papa = require('babyparse');

//Supports https requests. Prevents 'no-access-control-allow-origin' error.
const request = require('request');

/* Sets a limit to the number of promises running in parallel, which will improve scalability. This prevents 20,000+ requests from firing all at once. In this case we will set the limit for the maximum number of promise-returning functions that are allowed to run concurrently at 60. */
const queue = new (require('cwait').TaskQueue)(Promise, 30);

/* Function that returns a promise. The queue.wrap() wraps the function, so that before running it waits until the amount of concurrent invocations are below the queue's limit. */
const promisifiedRequest = queue.wrap((url, tacticId) => (

  /* Since we need to log both success and failure response codes, the promise should always resolve. For example, not resolving a connection timeout will result in the catch handler logging the error and stop further execution of code. */
  new Promise((resolve) => {

    //Wait 10 seconds for server to send response, if no response is sent, then treat as a connection timeout.
    request(url, { timeout: 10000 }, (error, response) => {

      /* If there is a connection timeout we resolve it here and return info we need about the request. We want to store the tactic_id and url of the current request so we can access this after promise fulfills. The information is needed for our final output. */
      if (error) {
        resolve({
          timedOut: true,
          tacticId,
          error,
          url
        });

      // This returns obj and handles all 2xx, 3xx, and 4xx responses.
      } else {
        resolve({
          tacticId,
          response,
          url
        });
      }
    });
  })
));

//If not a valid JSON string, return false instead of throwing an error. Else, return true.
const checkJSON = val => {
  try {
    JSON.parse(val);
  } catch (err) {
    return false;
  }
  return true;
};

//Handles case in tactic_id 325375 where impression_pixel_json is invalid due to missing a " before the http://.
const convertToJSON = str => {
  const output = str[0] + '"' + str.slice(1);
  return JSON.parse(output);
};

//Checks for valid json string. If valid, will return the parsed string, else return false.
const sanitize = jsonString => {
  const isValid = checkJSON(jsonString);

  if (isValid && jsonString !== '[]') {
    return JSON.parse(jsonString);
  }
  //Handles case for tactic_id 325375
  else if (jsonString[1] === 'h') {
    jsonString = convertToJSON(jsonString);
    return jsonString;
  } else {
    //Returns false for NULL and []
    return false;
  }
};

/* Takes in the arr of fulfilled promises as argument and returns the final output in an object. The returned object will have three keys: 1. failed - number of failed responses, 2. success - number of successful responses, 3. failedIds - an object with keys representing the tactic_id and values as an array of urls related to the key (some have more than 1 pixel url) */
const finalOutput = arr => (
  arr.reduce((obj, res) => {
    const failedIds = obj.failedIds;

    /* Checks for failed request. Since 4xx codes get returned in a different object, we first need to check if the object has a response key before trying to access the statusCode property of response value or else it will throw an error. */
    if (res.timedOut || ('response' in res && res.response.statusCode > 399)) {
      obj.failed++;

      /* Because there are duplicate tactic_ids, we first check if a tactic_id is already logged in the failedIds object. If true, we push the associated url into the array. If false we initialize it and place its url inside of an array. */
      failedIds[res.tacticId] ? failedIds[res.tacticId].push(res.url) : failedIds[res.tacticId] = [res.url];
    } else {
      obj.success++;
    }

    //Returns updated obj after each iteration.
    return obj;
  }, {
    //Initialized values for our output.
    failedIds: {},
    failed: 0,
    success: 0
  })
);

/* Reads file asynchronously. Non blocking, will not tie up the single thread loop and allow other functions to process. Binary encoding option is used so that it can properly process the csv. */
fs.readFile(csv, 'binary', (err, data) => {
  if (err) throw err;
  Papa.parse(data, {

    //Setting header to true will set the first row of csv as keys in an object.
    header: true,

    //Executes when parsing completes. Results represents an object with 3 keys: data, error, and meta.
    complete: (results) => {

      //Array of all request promises.
      let arrOfRequests = [];

      /* Results.data is an array of objects with each header name as its keys. Each object represent the impression. */
      results.data.forEach(impression => {
        const validUrlArr = sanitize(impression.impression_pixel_json);

        /* Check if array gets returned. Loop through impression pixels, create a promise for a request, and store it inside the array along with all requests */
        if (validUrlArr) {
          arrOfRequests = arrOfRequests.concat(validUrlArr.map(url => promisifiedRequest(url, impression.tactic_id)));
        }
      });

      /* Fire off requests under one condition. Queue prevents all requests from firing concurrently, instead, will wait until the number of concurrent tasks is below the queues limit(in this case 60) before sending another request. */
      return Promise.all(arrOfRequests)
      .then(arrOfFulfilledReqs => console.log(finalOutput(arrOfFulfilledReqs)))
      .catch(reason => console.error('Here is an error', reason));
    }
  });
});


//exported for testing
module.exports = {
  promisifiedRequest,
  checkJSON,
  sanitize,
  finalOutput
};