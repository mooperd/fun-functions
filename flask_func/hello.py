import pymysql
import config
from flask import Flask, jsonify
import pprint


app = Flask(__name__)

def get_db_connection():
    return

def query_db():
    connection = pymysql.connect(
                    host=config.db['host'],
                    port=config.db['port'],
                    db=config.db['db'],
                    user=config.db['properties']['user'],
                    password=config.db['properties']['password'],
                    charset=config.db['properties']['charset'],
                    cursorclass=pymysql.cursors.DictCursor
                                )
    try:
        with connection.cursor() as cursor:
            query = "SELECT `TheTable`.`timestamp`," \
                    "`TheTable`.`year`," \
                    "`TheTable`.`month`," \
                    "`TheTable`.`day`," \
                    "`TheTable`.`hour`," \
                    "`TheTable`.`minute`," \
                    "`TheTable`.`second`," \
                    "`TheTable`.`timezone`" \
                    "FROM `Time`.`TheTable`;"
            cursor.execute(query)
            result = cursor.fetchall()
            return result
    except:
        raise


@app.route('/')
def hello_world():
    cur = query_db()
    return jsonify(*cur)
