
hadoop = {
    'parquet.enable.summary-metadata': 'false',

    'fs.s3n.awsAccessKeyId':     'XXXXXXXXXXXXXXXXXXXXXX',
    'fs.s3n.awsSecretAccessKey': 'XXXXXXXXXXXXXXXXXXXXXX',
}

spark = {
    # this is no longer required since spark 1.6
    #'spark.storage.memoryFraction': 0.1,

    # Writing to partitioned parquet table can fail with OOM
    # see https://issues.apache.org/jira/browse/SPARK-12546
    'spark.sql.sources.maxConcurrentWrites': 1
}

paths = {
    'logs-piwik':  's3n://piwik-tracking-logs/',
    'logs-hdfs':   'maprfs:///user/dcmn/logs/',
    'visits-hdfs': 'maprfs:///user/dcmn/visits/',
    'geoip2_city': '/usr/share/GeoIP/GeoLite2-City.mmdb',
}

db = {
    'type': 'mysql',
    'host': '192.168.65.60',
    'port': 13306,
    'db':   'defaultdb',
    'properties': {
        'user':     'root',
        'password': 'root',
        'charset':  'utf8'
    }
}

def toJdbcUrl(db):
    url = 'jdbc:' + db['type']

    if 'host' in db:
        url = url + '://' + db['host']
        if 'port' in db:
            url = url + ':' + str(db['port'])

    if 'db' in db:
        url = url + '/' + db['db']

    return url
