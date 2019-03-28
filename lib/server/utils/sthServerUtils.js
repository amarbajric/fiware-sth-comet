/*
 * Copyright 2016 Telefónica Investigación y Desarrollo, S.A.U
 *
 * This file is part of the Short Time Historic (STH) component
 *
 * STH is free software: you can redistribute it and/or
 * modify it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * STH is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with STH.
 * If not, see http://www.gnu.org/licenses/.
 *
 * For those usages not covered by the GNU Affero General Public License
 * please contact with: [german.torodelvalle@telefonica.com]
 */

'use strict';

var ROOT_PATH = require('app-root-path');
var uuid = require('uuid');
var sthConfig = require(ROOT_PATH + '/lib/configuration/sthConfiguration');
var moment = require('moment-timezone');

/**
 * Returns the platform correlator if included in a request
 * @param {object} request The HTTP request
 * @return {string} The correlator, if any
 */
function getCorrelator(request) {
  return request && request.headers[sthConfig.HEADER.CORRELATOR];
}

/**
 * Adds the Fiware-Correlator header into the response object
 * @param {object} request  The request
 * @param {object} response The response
 */
function addFiwareCorrelator(request, response) {
  response.header(sthConfig.HEADER.CORRELATOR, getCorrelator(request) || request.sth.context.trans);
}

/**
 * Adds the Fiware-Total-Count header into the response object
 * @param {object} totalCount  The totalCount
 * @param {object} response The response
 */
function addFiwareTotalCount(totalCount, response) {
  response.header(sthConfig.HEADER.FIWARE_TOTAL_COUNT, totalCount);
}

/**
 * Generates the transaction identifier to be used for logging
 * @return {string} The generated transaction
 */
function createTransaction() {
  return uuid.v4();
}

/**
 * Returns the operation type for a concrete request to be used for logging
 * @param {object} request The request
 * @return {string} The operation type
 */
function getOperationType(request) {
  if (!request) {
    return sthConfig.OPERATION_TYPE.SERVER_LOG;
  } else {
    return sthConfig.OPERATION_TYPE_PREFIX + request.method.toUpperCase();
  }
}

/**
 * Returns the object to return in case no aggregated data exists for
 *  certain criteria
 * @returns {Array} An empty array
 */
function getEmptyResponse() {
  return [];
}

/**
 * Transforms a response payload into a NGSI formatted response
 *  payload
 * @param entityId The id of the requested entity's data
 * @param entityType The type of the requested entity's data
 * @param attrName The id of the requestedattribute's data
 * @param payload The payload to transform
 * @return {Object} The payload using NGSI format
 */
function getNGSIPayload(entityId, entityType, attrName, payload) {
  var ngsiResponse = {
    contextResponses: [
      {
        contextElement: {
          attributes: [
            {
              name: attrName,
              values: payload
            }
          ],
          id: entityId,
          isPattern: false,
          type: entityType
        },
        statusCode: {
          code: '200',
          reasonPhrase: 'OK'
        }
      }
    ]
  };
  return ngsiResponse;
}

/**
 * @IPPR
 * Returns one single contextElement Object that can be put into a NGSIv1 Payload
 * @param entityId The id of the requested entity's data
 * @param entityType The type of the requested entity's data
 * @param attributes Array<{name: string, values: []}>
 * @return {Object} with the format of one single contextElement containing multiple attributes
 */
function getNGSIMultiAttributesContextElement(entityId, entityType, attributes) {
    return {
        contextElement: {
            attributes: attributes,
            id: entityId,
            isPattern: false,
            type: entityType
        },
        statusCode: {
            code: '200',
            reasonPhrase: 'OK'
        }
    };
}

/**
 * @IPPR
 * Transforms a response payload into a NGSI formatted response
 *  payload with multiple contextElement's and each contextElement
 *  has the possibility to contain multiple attributes
 * @param contextElements Array<contextElement>
 * @return {Object} The payload using NGSI format
 */
function getNGSIMultiContextElementsPayload(contextElements) {
    return {
        contextResponses: contextElements
    };
}

/**
 *  @IPPR - LWJ (LightWeight Json) Implementation
 * @param attrName string
 * @param fields Array<string> -> containing all field names
 * @param values Array<any> -> containing actual data
 * @returns {Object} One single attribute payload that can be used for alternative formats.
 */
function getLWJSingleAttributeResult(attrName, fields, values) {
    return {
        name: attrName,
        fields: fields,
        values: values
    };
}


/**
 * @IPPR - LWJ (LightWeight Json) Implementation
 * This object combines multiple single attribute results under
 * the 'attributes' property coming from the 'getLWJSingleAttributeResult' method.
 * @param entityId string
 * @param entityType string
 * @param attributes Array<{name: string, fields: [], values: []}>}
 * @returns {Object} One single result that can be used for alternative formats.
 */
function getLWJContextElement(entityId, entityType, attributes) {
    return {
        entityId: entityId,
        entityType: entityType,
        attributes: attributes
    };
}


/**
 * @IPPR - LWJ (LightWeight Json) Implementation
 * This completes a LWJ (LightWeight Json) request and represents one or more contextElements coming from
 * the 'getLWJContextElement' method.
 * @param results Array<{entityId: string, entityType: string, attributes: Array<{name: string, fields: [], values: []}>}>
 * @returns {Object} The payload using a custom format
 */
function getLWJPayload(results) {
    return {
        results: results
    };
}

/**
 * @IPPR - TimeZoneOffsetMillis Function
 * This method calculates the offset of the given dateTime in UTC for the given timezone
 * (see moment.js timezones) in milliseconds
 * @param utcDateTime Date
 * @param timezone string (e.g. 'Europe/Vienna')
 * @returns number The offset in milliseconds
 */
function timeZoneOffsetMillis(utcDateTime, timezone) {
    !timezone ? timezone = 'Europe/Vienna' : null;
    const utcDate = moment.utc(utcDateTime);
    const local = moment.utc(utcDate).tz(timezone);
    const offset = local.utcOffset(); //in minutes
    return offset * 60000;
};


/**
 * @IPPR - AggregationPipelineBuilder
 * This method builds a native aggregation pipeline especially for the aggregation functionality like avg,min and max
 * and the associated aggregation periods day,month,hour
 * Should only be used with mongodb version >= 4.x.x
 * @param aggregationMethod string (e.g. 'avg', 'min', 'max')
 * @param aggregationPeriod string (e.g. 'hour', 'day', 'month', null)
 * @param hLimit number (e.g. 3000, 4000, null)
 * @param attrName string (e.g. 'temperature', 'humidity')
 * @param dateFrom Date
 * @param dateTo Date
 * @returns Array<Object> The native aggregation pipeline ready to be used with the mongo db.aggregate() method
 */
function aggregationPipelineBuilder(aggregationMethod, aggregationPeriod, hLimit, attrName, dateFrom, dateTo) {
    /* a mapping for the $substr function in order to trim the date for the given aggrPeriod or either use
    * $attrName if no period is specified (e.g. 'hour' trims the ISODate at position 13 to include hours but
    * exclude minutes and seconds) */
    const subStrAggrPeriodMap = {'hour': 13, 'day': 10, 'month': 7, 'none': '$attrName'};
    const aggregationObject = JSON.parse('{"$' + aggregationMethod + '": "$value"}');
    Object.keys(subStrAggrPeriodMap).indexOf(aggregationPeriod) < 0 ? aggregationPeriod = 'none' : null;
    if (aggregationPeriod === 'none' && hLimit < 0) {
        throw new Error('AggregationPipelineBuilder: hLimit has to be positive if aggregationPeriod is not being used');
    }
    const aggregationPipeline = [
        {'$match':
                {'$and': [
                        {'attrName': attrName},
                        {'recvTime' : {'$gte': dateFrom}},
                        {'recvTime' : {'$lte': dateTo}}
                    ]
                }
        },
        {'$limit': hLimit},
        {'$project': {
                'recvTime': 1,
                //groupKey = yyyy-mm || yyyy-mm-dd || yyyy-mm-ddTHH || attrName
                'groupKey': aggregationPeriod === 'none' ? subStrAggrPeriodMap[aggregationPeriod] :
                    {'$substr': ['$recvTime', 0, subStrAggrPeriodMap[aggregationPeriod]]},
                'value': { '$toDouble': '$attrValue'},
                'attrName':1}
        },
        {'$group':
                {
                    '_id': '$groupKey',
                    'firstDate': {'$first': '$recvTime'},
                    'attrValue': aggregationObject
                }
        },
        {'$sort': { '_id': 1 } }
    ];
    aggregationPeriod !== 'none' ? aggregationPipeline.splice(1, 1) : null; // removes the {$limit: hLimit} pipeline
    return aggregationPipeline;
}


/**
 * Returns the logging context associated to a request
 * @param {Object} request The request received
 * @return {Object} The context to be used for logging
 */
function getContext(request) {
  var transactionId = createTransaction();
  return {
    corr: getCorrelator(request) || transactionId,
    trans: transactionId,
    op: getOperationType(request),
    from: request.headers[sthConfig.HEADER.X_REAL_IP] || 'n/a',
    srv: request.headers[sthConfig.HEADER.FIWARE_SERVICE],
    subsrv: request.headers[sthConfig.HEADER.FIWARE_SERVICE_PATH],
    comp: 'STH'
  };
}

module.exports = {
    addFiwareCorrelator: addFiwareCorrelator,
    addFiwareTotalCount: addFiwareTotalCount,
    getContext: getContext,
    getCorrelator: getCorrelator,
    getEmptyResponse: getEmptyResponse,
    getNGSIPayload: getNGSIPayload,
    getNGSIMultiAttributesContextElement: getNGSIMultiAttributesContextElement,
    getNGSIMultiContextElementsPayload: getNGSIMultiContextElementsPayload,
    getLWJSingleAttributeResult: getLWJSingleAttributeResult,
    getLWJContextElement: getLWJContextElement,
    getLWJPayload: getLWJPayload,
    timeZoneOffsetMillis: timeZoneOffsetMillis,
    aggregationPipelineBuilder: aggregationPipelineBuilder,
};
