const request = require('request');
const fs = require('fs');
const chai = require('chai');
const Papa = require('babyparse');
const { promisifiedRequest, checkJSON, sanitize, finalOutput } = require('./index');
const test = './test.csv';
const chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);
const expect = chai.expect;


const csvFile = fs.readFileSync(test, 'binary');

describe('Unit Test', () => {
  const successURL = 'https://ad.doubleclick.net/ddm/ad/N7676.791086DOUBLECLICKTECH.COM/B9352239.127304136;sz=1x1;ord=[timestamp];dc_lat=;dc_rdid=;tag_for_child_directed_treatment=?';
  const failedURL = 'http://servedby.flashtalking.com/imp/8/59504;1761063;201;pixel;TripleLift;TripleLiftProspectingAzul1x1/?cachebuster=[CACHEBUSTER]';
  const timeout = 'https://ohlone.vizu.com/a.gif?vzcid=19761&vzadid=triplelift&vzsid=accuen&ord=[RANDOM]';
  let impressions;
  let impression;
  Papa.parse(csvFile, {
    header: true,
    complete: (results) => {
      impressions = results;
      impression = results.data[0];
    }
  });

  describe('CSV', () => {

    it('should be an object', () => {
      expect(impressions).to.be.an('object');
    });
    it('should have a property data which contains 543 entries', () => {
      expect(impressions).to.have.property('data').with.lengthOf(543);
      expect(impressions.data).to.be.an('array');
    });
    it('each element inside array should be objects with 13 keys', () => {
      expect(Object.keys(impressions.data[0]).length).to.equal(13);
      expect(impressions.data[0]).to.be.an('object');
    });
    it('should contain keys named impression_pixel_json and tactic_id', () => {
      expect(impressions.data[0]).to.have.property('impression_pixel_json');
      expect(impressions.data[0]).to.have.property('tactic_id');
    });
    it('tactic_id of first impression should be 333304', () => {
      expect(+impressions.data[0].tactic_id).to.be.an('number');
      expect(+impressions.data[0].tactic_id).to.equal(333304);
    });
  });

  describe('Requests', () => {
    it('sends back a response', () => {
      request(successURL, (err, response) => {
        if (err) throw err;
        expect(response).to.be.an('object');
      });
    });
    it('contains statusCode key and responds with 200 if successful', () => {
      request(successURL, (err, response) => {
        if (err) throw err;
        expect(response).to.have.property('statusCode');
        expect(response.statusCode).to.equal(200);
      });
    });
    it('responds with 404', () => {
      request(failedURL, (err, response) => {
        if (err) throw err;
        expect(response).to.have.property('statusCode');
        expect(response.statusCode).to.equal(404);
      });
    });
  });

  describe('Functions', () => {

    describe('promisifiedRequest', () => {

      const promiseRequest = promisifiedRequest(successURL, impression);
      const promiseRequest2 = promisifiedRequest(failedURL, impression);
      const promiseRequest3 = promisifiedRequest(timeout, impression);


      it('should return a promise', () => {
        expect(promiseRequest).to.be.an.instanceof(Promise);
      });
      it('promise should return an object', () => {
        expect(Promise.resolve(promiseRequest)).to.eventually.be.an('object');
      });
      it('object should have a tacticId property', () => {
        expect(Promise.resolve(promiseRequest)).to.eventually.have.property(['tacticId']);
      });
      it('should still resolve with 404 response', () => {
        expect(Promise.resolve(promiseRequest2)).to.eventually.have.property(['response']);
      });
      it('requests that time out should resolve in the error object', () => {
        expect(Promise.resolve(promiseRequest3)).to.eventually.have.property(['error']);
      });
      it('resolved promises through the success object should not include error property', () => {
        expect(Promise.resolve(promiseRequest2)).to.eventually.not.have.property(['error']);
      });
    });


    describe('checkJSON', () => {
      const validJsonString = JSON.stringify([1, 2, 3, 4, 5]);
      const invalidJson = [];

      it('returns true for valid JSON', () => {
        expect(checkJSON(validJsonString)).to.equal(true);
      });
      it('returns false for invalid JSON', () => {
        expect(checkJSON(invalidJson)).to.equal(false);
      });
    });

    describe('sanitize', () => {
      const jsonStr = JSON.stringify([{color: 'blue', food: 'pizza'}]);
      const invalidJson = [];

      it('returns JS value if input is a valid JSON string', () => {
        expect(sanitize(jsonStr)).to.be.an('array');
      });
      it('returns false for invalid JSON', () => {
        expect(sanitize(invalidJson)).to.equal(false);
      });
    });

    describe('finalOutput', () => {

      const resolvedObj = {
        tacticId: 1,
        response: 200,
        url: 'google.com'
      };

      const timeoutObj = {
        timedOut: true,
        tacticId: 2,
        error: 500,
        url: 'facebook.com'
      };

      const failedObj = {
        tacticId: 3,
        response: {
          statusCode: 404
        },
        url: 'math.com'
      };

      const arr = [resolvedObj, timeoutObj, failedObj];

      const output = finalOutput(arr);

      it('returns an object', () => {
        expect(finalOutput(arr)).to.be.an('object');
      });
      it('has failedIds, failed, and success as keys', () => {
        expect(Object.keys(output).length).to.equal(3);
        expect(Object.keys(output)).to.be.deep.equal(['failedIds', 'failed', 'success']);
      });
      it('should have two failed and one success', () => {
        expect(output.failed).to.equal(2);
        expect(output.success).to.equal(1);
      });
      it('failedIds should be an object with two keys', () => {
        expect(output.failedIds).to.be.an('object');
        expect(Object.keys(output.failedIds).length).to.equal(2);
      });
      it('failedIds should contain tacticId 3', () => {
        expect(output.failedIds).to.include.keys('3');
      });
    });
  });
});