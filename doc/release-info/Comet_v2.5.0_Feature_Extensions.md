# Feature Description for sth-comet release version 2.5.0

## New Features
- The existing API was extended and now also features the possibility to:
  - retrieve multiple entityId's (i.e. sensors) for **one** entityType (i.e. traffic, mobile, static) within one request/response
  - retrieve multiple attributes (i.e. sensor measurements) for each requested entityId within one request/response
  - switch between the JSON format of the response (NGSIv1 and a new more lightweight format)
 
## API Extension for multi entityId and attribute requests
In order to retrieve multiple entityId's and/or multiple attributes with one single request, the following syntax applies:
`GET /STH/v1/contextEntities/type/{entityType}/id/{entityIds}/attributes/{attrNames}?{queryParams}`
- **{entityType}**: The entityType data is requested for. Keep in mind that only one entityTpe can be retrieved
with one request.
- **{entityIds}**: One or more entityId's data is requested for. If only a single entityId is requested, the 
placeholder is replaced by the name of the entityId. If however multiple entityId's are requested,
the placeholder has to be replaced by a comma-separated string of entityId's.
- **{attrNames}**: One or more attributes data is requested for. If only a single attribute is requested, the 
placeholder is replaced by the name of the attribute. If however multiple attributes are requested,
the placeholder has to be replaced by a comma-separated string of attribute names.
  - Each attribute is requested for every given entityId. This means that for **n** given attributes,
  the comet will return data of **n** attributes **for each given entityId if it exists**. If an attribute
  does not exist for a given entityId, it is simply ignored and won't throw any error.
- **{queryParams}**: Besides of the comet supporting multiple query parameters, for a request containing 
one or more entityId's/attributes, the following query parameters are relevant and can be found in the 
[sth-comet documentation](https://fiware-sth-comet.readthedocs.io/en/latest/raw-data-retrieval/index.html):
  - dateFrom
  - dateTo
  - lastN
  - hLimit
  - hOffset
  - count
  - nongsi (new query parameter)

### The `nongsi` query parameter
This query parameter, that was added to the available list of parameters, can be **exclusively** used to 
control the JSON format of a multi entityId and attribute response. By setting the parameter to `true` 
(e.g. `?nongsi=true`), a more ligthweight JSON format is used for the response data compared to the default
NGSIv1 format. If the parameter is left out or set to `false`, the default NGSIv1 format is being used.

### The `hLimit `query parameter
For a multi entityId and attribute request, the `hLimit` query parameter sets a limit on how much data points
are being queried from the database. Note that the `hLimit` parameter applies for every single attribute that
is being requested. This means, that for **n** given attributes, **n** * `hLimit` datapoints will be returned 
instead of only returning `hLimit` datapoints.

## Examples
### Example 1: Multiple entityId's with one attribute
`GET http://sth-comet:8666/STH/v1/contextEntities/type/static/id/Sensor1,Sensor2/attributes/temperature?dateFrom=2018-10-01T00:00:00.000Z&dateTo=2018-10-30T23:59:59.000Z&hLimit=5&hOffset=0`

#### Response (NGSIv1 format)
```
{
    "contextResponses": [
        {
            "contextElement": {
                "attributes": [
                    {
                        "name": "temperature",
                        "values": [
                            {
                                "recvTime": "2018-10-01T00:00:05.000Z",
                                "attrType": "float",
                                "attrValue": "5.23"
                            },
                            {
                                "recvTime": "2018-10-01T00:00:35.000Z",
                                "attrType": "float",
                                "attrValue": "5.23"
                            }
                        ]
                    }
                ],
                "id": "Sensor1",
                "isPattern": false,
                "type": "static"
            },
            "statusCode": {
                "code": "200",
                "reasonPhrase": "OK"
            }
        },
        {
            "contextElement": {
                "attributes": [
                    {
                        "name": "temperature",
                        "values": [
                            {
                                "recvTime": "2018-10-01T00:00:17.000Z",
                                "attrType": "float",
                                "attrValue": "5.29"
                            },
                            {
                                "recvTime": "2018-10-01T00:00:47.000Z",
                                "attrType": "float",
                                "attrValue": "5.27"
                            }
                        ]
                    }
                ],
                "id": "Sensor2",
                "isPattern": false,
                "type": "static"
            },
            "statusCode": {
                "code": "200",
                "reasonPhrase": "OK"
            }
        }
    ]
}
```

### Example 2: One entityId with mutliple attributes
`GET http://sth-comet:8666/STH/v1/contextEntities/type/static/id/Sensor1/attributes/temperature,humidity?dateFrom=2018-10-01T00:00:00.000Z&dateTo=2018-10-30T23:59:59.000Z&hLimit=5&hOffset=0`

#### Response (NGSIv1 format)
```
{
    "contextResponses": [
        {
            "contextElement": {
                "attributes": [
                    {
                        "name": "humidity",
                        "values": [
                            {
                                "recvTime": "2018-10-01T00:00:05.000Z",
                                "attrType": "float",
                                "attrValue": "92.91"
                            },
                            {
                                "recvTime": "2018-10-01T00:00:35.000Z",
                                "attrType": "float",
                                "attrValue": "92.96"
                            }
                        ]
                    },
                    {
                        "name": "temperature",
                        "values": [
                            {
                                "recvTime": "2018-10-01T00:00:05.000Z",
                                "attrType": "float",
                                "attrValue": "5.23"
                            },
                            {
                                "recvTime": "2018-10-01T00:00:35.000Z",
                                "attrType": "float",
                                "attrValue": "5.23"
                            }
                        ]
                    }
                ],
                "id": "Sensor1",
                "isPattern": false,
                "type": "static"
            },
            "statusCode": {
                "code": "200",
                "reasonPhrase": "OK"
            }
        }
    ]
}
```

### Example 3: Multiple entityId's with mutliple attributes
`GET http://sth-comet:8666/STH/v1/contextEntities/type/static/id/Sensor1,Sensor2/attributes/temperature,humidity?dateFrom=2018-10-01T00:00:00.000Z&dateTo=2018-10-30T23:59:59.000Z&hLimit=5&hOffset=0`

#### Response (NGSIv1 format)
```
{
    "contextResponses": [
        {
            "contextElement": {
                "attributes": [
                    {
                        "name": "humidity",
                        "values": [
                            {
                                "recvTime": "2018-10-01T00:00:05.000Z",
                                "attrType": "float",
                                "attrValue": "92.91"
                            },
                            {
                                "recvTime": "2018-10-01T00:00:35.000Z",
                                "attrType": "float",
                                "attrValue": "92.96"
                            }
                        ]
                    },
                    {
                        "name": "temperature",
                        "values": [
                            {
                                "recvTime": "2018-10-01T00:00:05.000Z",
                                "attrType": "float",
                                "attrValue": "5.23"
                            },
                            {
                                "recvTime": "2018-10-01T00:00:35.000Z",
                                "attrType": "float",
                                "attrValue": "5.23"
                            }
                        ]
                    }
                ],
                "id": "Sensor1",
                "isPattern": false,
                "type": "static"
            },
            "statusCode": {
                "code": "200",
                "reasonPhrase": "OK"
            }
        },
        {
            "contextElement": {
                "attributes": [
                    {
                        "name": "humidity",
                        "values": [
                            {
                                "recvTime": "2018-10-01T00:00:17.000Z",
                                "attrType": "float",
                                "attrValue": "90.02"
                            },
                            {
                                "recvTime": "2018-10-01T00:00:47.000Z",
                                "attrType": "float",
                                "attrValue": "90.14"
                            }
                        ]
                    },
                    {
                        "name": "temperature",
                        "values": [
                            {
                                "recvTime": "2018-10-01T00:00:17.000Z",
                                "attrType": "float",
                                "attrValue": "5.29"
                            },
                            {
                                "recvTime": "2018-10-01T00:00:47.000Z",
                                "attrType": "float",
                                "attrValue": "5.27"
                            }
                        ]
                    }
                ],
                "id": "Sensor2",
                "isPattern": false,
                "type": "static"
            },
            "statusCode": {
                "code": "200",
                "reasonPhrase": "OK"
            }
        }
    ]
}
```

### Example 4: Multiple entityId's with mutliple attributes and `nongsi=true`
`GET http://sth-comet:8666/STH/v1/contextEntities/type/static/id/Sensor1,Sensor2/attributes/temperature,humidity?dateFrom=2018-10-01T00:00:00.000Z&dateTo=2018-10-30T23:59:59.000Z&hLimit=5&hOffset=0&nongsi=true`

#### Response (Lightweight JSON format)
```
{
    "results": [
        {
            "entityId": "Sensor1",
            "entityType": "static",
            "attributes": [
                {
                    "name": "humidity",
                    "fields": [
                        "recvTime",
                        "attrType",
                        "attrValue"
                    ],
                    "values": [
                        [
                            "2018-10-01T00:00:05.000Z",
                            "float",
                            "92.91"
                        ],
                        [
                            "2018-10-01T00:00:35.000Z",
                            "float",
                            "92.96"
                        ]
                    ]
                },
                {
                    "name": "temperature",
                    "fields": [
                        "recvTime",
                        "attrType",
                        "attrValue"
                    ],
                    "values": [
                        [
                            "2018-10-01T00:00:05.000Z",
                            "float",
                            "5.23"
                        ],
                        [
                            "2018-10-01T00:00:35.000Z",
                            "float",
                            "5.23"
                        ]
                    ]
                }
            ]
        },
        {
            "entityId": "Sensor2",
            "entityType": "static",
            "attributes": [
                {
                    "name": "temperature",
                    "fields": [
                        "recvTime",
                        "attrType",
                        "attrValue"
                    ],
                    "values": [
                        [
                            "2018-10-01T00:00:17.000Z",
                            "float",
                            "5.29"
                        ],
                        [
                            "2018-10-01T00:00:47.000Z",
                            "float",
                            "5.27"
                        ]
                    ]
                },
                {
                    "name": "humidity",
                    "fields": [
                        "recvTime",
                        "attrType",
                        "attrValue"
                    ],
                    "values": [
                        [
                            "2018-10-01T00:00:17.000Z",
                            "float",
                            "90.02"
                        ],
                        [
                            "2018-10-01T00:00:47.000Z",
                            "float",
                            "90.14"
                        ]
                    ]
                }
            ]
        }
    ]
}
```

### Example 5: NGSIv1 vs. Lightweight JSON
Considering two requests where two entityId's and two attributes with a `hLimit` of 1000 (i.e. 1000 * 2 = 2000 datapoints) are requested once with `nongsi` set to `true` and once set to `false`, the following results can be observed:

**`nongsi` set to true**:
`GET http://sth-comet:8666/STH/v1/contextEntities/type/static/id/Sensor1,Sensor2/attributes/temperature,humidity?dateFrom=2018-10-01T00:00:00.000Z&dateTo=2018-10-30T23:59:59.000Z&hLimit=1000&hOffset=0&nongsi=true`

**`nongsi` set to false**:
`GET http://sth-comet:8666/STH/v1/contextEntities/type/static/id/Sensor1,Sensor2/attributes/temperature,humidity?dateFrom=2018-10-01T00:00:00.000Z&dateTo=2018-10-30T23:59:59.000Z&hLimit=1000&hOffset=0&nongsi=false`

**NOTE:** The example was performed on a local configuration where the comet was running on the localhost and interacted with a mongo instance running on a kubernetes cluster hosted by minikube. POSTMAN was used for the client side.

- NGSIv1 format
  - Response has a size of `307.88 KB`
  - Response takes `1328 ms` (TTLB = Time To Last Byte)
- Lightweight format
  - Response has a size of `175.09 KB`
  - Response takes `1226 ms` (TTLB = Time To Last Byte)




