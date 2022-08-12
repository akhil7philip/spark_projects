# Output

## producer.py
`producing message %s for topic %s {'t': 1660296970059, 'id': 'R', 'v': 0} test_314`

`producing message %s for topic %s {'t': 1660296971356, 'id': 'F', 'v': 4} test_314`

`producing message %s for topic %s {'t': 1660296972360, 'id': 'Y', 'v': 7} test_314`

`producing message %s for topic %s {'t': 1660296973365, 'id': 'K', 'v': 9} test_314`

`producing message %s for topic %s {'t': 1660296974367, 'id': 'K', 'v': 7} test_314`

`producing message %s for topic %s {'t': 1660296975368, 'id': 'T', 'v': 1} test_314`

`producing message %s for topic %s {'t': 1660296976371, 'id': 'C', 'v': 1} test_314`

`producing message %s for topic %s {'t': 1660296977373, 'id': 'R', 'v': 6} test_314`

`producing message %s for topic %s {'t': 1660296978376, 'id': 'W', 'v': 7} test_314`

`producing message %s for topic %s {'t': 1660296979379, 'id': 'W', 'v': 6} test_314`

`producing message %s for topic %s {'t': 1660296980380, 'id': 'J', 'v': 7} test_314`

`producing message %s for topic %s {'t': 1660296981381, 'id': 'A', 'v': 0} test_314`

`producing message %s for topic %s {'t': 1660296982384, 'id': 'P', 'v': 2} test_314`

`producing message %s for topic %s {'t': 1660296983387, 'id': 'E', 'v': 3} test_314`

`producing message %s for topic %s {'t': 1660296984392, 'id': 'V', 'v': 1} test_314`

`producing message %s for topic %s {'t': 1660296985397, 'id': 'F', 'v': 6} test_314`

`producing message %s for topic %s {'t': 1660296986400, 'id': 'I', 'v': 7} test_314`

`producing message %s for topic %s {'t': 1660296987402, 'id': 'S', 'v': 0} test_314`

`producing message %s for topic %s {'t': 1660296988403, 'id': 'Q', 'v': 7} test_314`

`producing message %s for topic %s {'t': 1660296989405, 'id': 'P', 'v': 4} test_314`

`producing message %s for topic %s {'t': 1660296990409, 'id': 'R', 'v': 9} test_314`

`producing message %s for topic %s {'t': 1660296991413, 'id': 'R', 'v': 6} test_314`

`producing message %s for topic %s {'t': 1660296992415, 'id': 'B', 'v': 1} test_314`

`producing message %s for topic %s {'t': 1660296993420, 'id': 'Y', 'v': 8} test_314`

`producing message %s for topic %s {'t': 1660296994421, 'id': 'Y', 'v': 7} test_314`


....

#

## main.py
....

`Setting default log level to "WARN".`

`To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).`

`INFO: Successful created Spark Session`

`INFO: Reading data from source topic`

`22/08/12 15:57:46 WARN ResolveWriteToStream: Temporary checkpoint location created which is deleted normally when the query didn't fail: /private/var/folders/md/k1d887kx4mn3gynrgncffwghjp85n1/T/temporary-33aca105-f645-4db6-acc8-703788eb1f7e. If it's required to delete it under any circumstances, please set spark.sql.streaming.forceDeleteTempCheckpointLocation to true. Important to know deleting temp checkpoint folder is best effort.`

`22/08/12 15:57:46 WARN ResolveWriteToStream: spark.sql.adaptive.enabled is not supported in streaming DataFrames/Datasets and will be disabled.`

`INFO: Successfully received data`

`INFO: Completed basic filtering and transformation of data`

`time taken transform and push msg: 0.2261928779999991s                          `

`----------------------------------------------`

`INFO: Successfully received data`

`INFO: Completed basic filtering and transformation of data`

`time taken transform and push msg: 0.22521439600001258s                         `

`----------------------------------------------`

`INFO: Successfully received data`

`INFO: Completed basic filtering and transformation of data`

`time taken transform and push msg: 0.22454711100004943s                         `

`----------------------------------------------`

`INFO: Successfully received data`

`INFO: Completed basic filtering and transformation of data`

`time taken transform and push msg: 0.22397165899997162s                         `

`----------------------------------------------`

`INFO: Successfully received data`

`INFO: Completed basic filtering and transformation of data`

`time taken transform and push msg: 0.2159552109999936s                          `

`----------------------------------------------`

`INFO: Successfully received data`

`INFO: Completed basic filtering and transformation of data`

`time taken transform and push msg: 0.21934336399999665s`

`----------------------------------------------`


....

#

## consumer.py
`starting the consumer`

`{'date': '2022-08-12', 'id': 'W', 'v': 76}`

`{'date': '2022-08-12', 'id': 'E', 'v': 95}`

`{'date': '2022-08-12', 'id': 'R', 'v': 133}`

`{'date': '2022-08-12', 'id': 'T', 'v': 69}`

`{'date': '2022-08-12', 'id': 'Q', 'v': 95}`

`{'date': '2022-08-12', 'id': 'W', 'v': 76}`

`{'date': '2022-08-12', 'id': 'E', 'v': 95}`

`{'date': '2022-08-12', 'id': 'R', 'v': 133}`

`{'date': '2022-08-12', 'id': 'T', 'v': 69}`

`{'date': '2022-08-12', 'id': 'Q', 'v': 95}`

`{'date': '2022-08-12', 'id': 'W', 'v': 2}`

`{'date': '2022-08-12', 'id': 'E', 'v': 11}`

`{'date': '2022-08-12', 'id': 'Q', 'v': 2}`

`{'date': '2022-08-12', 'id': 'E', 'v': 3}`

`{'date': '2022-08-12', 'id': 'R', 'v': 12}`

`{'date': '2022-08-12', 'id': 'Q', 'v': 7}`
