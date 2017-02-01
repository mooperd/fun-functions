import pymysql
import pymysql.converters
import config

class DbWriter:

    @staticmethod
    def insert(df, table, column_expressions = {}, options = {}):
        """
        Insert the given dataframe into the given table

        in: df                 pyspark.sql.DataFrame
        in: table              str  The name of the destination name
        in: column_expressions dict An optional map of column name and SQL expression
                                    In case the column name is a known one
                                    then the quoted value will used for formating the expression.
                                    (expression = expression.format(quoted_raw_val))
        in: options            dict An optional map of options:
                                    Option Name     | Default Value          | Description
                                    -----------------------------------------------------------
                                    use_transaction | False                  | Run queries in a transaction
                                    max_batch_size  | None                   | Max. numbers of inserts send as once
                                    max_query_size  | max_allowed_packet - 4 | Max. query size in bytes
        """
        columns = DbWriter._get_columns(df, column_expressions)
        options = DbWriter._get_options(options)

        df.rdd.foreachPartition(lambda iterator: DbWriter._insert_partition(iterator, table, columns, column_expressions, options))

    @staticmethod
    def upsert(df, table, column_expressions = {}, options = {}):
        """
        Upsert the given dataframe into the given table.
        Upsert means INSERT ... ON DUPLICATE KEY UPDATE ...

        in: df                 pyspark.sql.DataFrame
        in: table              str  The name of the destination name
        in: column_expressions dict An optional map of column name and SQL expression
                                    In case the column name is a known one
                                    then the quoted value will used for formating the expression.
                                    (expression = expression.format(quoted_raw_val))
        in: options            dict An optional map of options:
                                    Option Name     | Default Value          | Description
                                    -----------------------------------------------------------
                                    use_transaction | False                  | Run queries in a transaction
                                    max_batch_size  | None                   | Max. numbers of inserts send as once
                                    max_query_size  | max_allowed_packet - 4 | Max. query size in bytes
        """
        columns = DbWriter._get_columns(df, column_expressions)
        options = DbWriter._get_options(options)

        sql_suffix = 'ON DUPLICATE KEY UPDATE ' + ','.join(['`' + col + '`=VALUES(`' + col + '`)' for col in columns])

        df.rdd.foreachPartition(lambda iterator: DbWriter._insert_partition(iterator, table, columns, column_expressions, options, sql_suffix))

    @staticmethod
    def overwrite(df, table, column_expressions = {}, options = {}):
        """
        Overwrite the given table with the given dataframe.

        This method first creates a new table with the same schema as the given one
        and inserts the hole dataframe into this new table. After success it will
        switch the old table with the new one and drops the old table.

        in: df                 pyspark.sql.DataFrame
        in: table              str  The name of the destination name
        in: column_expressions dict An optional map of column name and SQL expression
                                    In case the column name is a known one
                                    then the quoted value will used for formating the expression.
                                    (expression = expression.format(quoted_raw_val))
        in: options            dict An optional map of options:
                                    Option Name     | Default Value          | Description
                                    -----------------------------------------------------------
                                    use_transaction | False                  | Run queries in a transaction
                                    max_batch_size  | None                   | Max. numbers of inserts send as once
                                    max_query_size  | max_allowed_packet - 4 | Max. query size in bytes
        """

        new_table = 'tmp._new_' + table
        old_table = 'tmp._old_' + table

        conn    = DbWriter._get_connection()
        columns = DbWriter._get_columns(df, column_expressions)
        options = DbWriter._get_options(options, conn)

        # create a new temporary talbe to insert to
        with conn.cursor() as cursor:
            cursor.execute('DROP TABLE IF EXISTS ' + new_table)
            cursor.execute('CREATE TABLE ' + new_table + ' LIKE ' + table)
        conn.close()
        del conn

        # insert into the new created temporary table
        try:
            df.rdd.foreachPartition(lambda iterator: DbWriter._insert_partition(iterator, new_table, columns, column_expressions, options))
        except:
            conn = DbWriter._get_connection()
            with conn.cursor() as cursor:
                cursor.execute('DROP TABLE ' + new_table)
            conn.close()
            raise

        # switch the new created temporary table into the real table and drop the old one
        conn = DbWriter._get_connection()
        with conn.cursor() as cursor:
            cursor.execute('RENAME TABLE ' + table + ' TO ' + old_table + ', ' + new_table + ' TO ' + table)
            cursor.execute('DROP TABLE ' + old_table)
        conn.close()

    @staticmethod
    def _get_options(options, conn = None):
        options['use_transaction'] = False if 'use_transaction' not in options else options['use_transaction']
        options['max_batch_size']  = None if 'max_batch_size' not in options else options['max_batch_size']
        options['max_query_size']  = None if 'max_query_size' not in options else options['max_query_size']
        options['mysql_charset']   = config.db['properties']['charset']

        # auto detect max_query_size
        # max_query_size = max_allowed_packet - 4
        if options['max_query_size'] is None:
            use_conn = conn if conn is not None else DbWriter._get_connection()
            with use_conn.cursor() as cursor:
                cursor.execute('SELECT @@max_allowed_packet max_allowed_packet')
                options['max_query_size'] = int(cursor.fetchone()['max_allowed_packet']) - 4
            if conn is None:
                use_conn.close()

        return options

    @staticmethod
    def _get_columns(df, column_expressions):
        columns = df.columns
        for col in column_expressions:
            if col not in columns:
                columns.append(col)
        return columns

    @staticmethod
    def _get_connection():
        return pymysql.connect(host=config.db['host'],
                               port=config.db['port'],
                               db=config.db['db'],
                               user=config.db['properties']['user'],
                               password=config.db['properties']['password'],
                               charset=config.db['properties']['charset'],
                               cursorclass=pymysql.cursors.DictCursor)

    @staticmethod
    def _insert_partition(items, table, columns, column_expressions, options, sql_suffix = ''):
        mysql_charset   = options['mysql_charset']
        max_batch_size  = options['max_batch_size']
        max_query_size  = options['max_query_size']
        use_transaction = options['use_transaction']

        conn   = None
        cursor = None
        def execute(sql):
            nonlocal conn
            nonlocal cursor

            if conn is None:
                conn   = DbWriter._get_connection()
                cursor = conn.cursor()
                if use_transaction:
                    conn.begin()

            cursor.execute(sql)

        def quote(v):
            if isinstance(v, str):
                return "'" + pymysql.converters.escape_string(v) + "'"
            return pymysql.converters.escape_item(v, mysql_charset)

        try:
            sql_cols        = ','.join(['`' + col + '`' for col in columns])
            sql_header      = 'INSERT INTO ' + table + '(' + sql_cols + ')VALUES'
            sql_size_header = len(sql_header)
            sql_size_suffix = len(sql_suffix)
            sql_size        = sql_size_header + sql_size_suffix
            sql_rows        = []

            for i, row in enumerate(items, 1):
                row_dict = row.asDict(True)
                row_list = []
                for col in columns:
                    if col in row_dict:
                        val = quote(row_dict[col])
                        if col in column_expressions:
                            val = column_expressions[col].format(val)
                    else:
                        val = column_expressions[col]

                    row_list.append(val)

                sql_row = ''.join(['(', ','.join(row_list), ')'])

                if (max_batch_size is not None and (i % max_batch_size) == 0) or (sql_size + len(sql_row)) > max_query_size:
                    sql = sql_header + ",".join(sql_rows) + sql_suffix
                    print("inserting " + str(len(sql_rows)) + " rows (" + str(len(sql)) + " bytes) ... ", end='')
                    execute(sql)
                    print('OK')

                    sql_size = sql_size_header + sql_size_suffix
                    sql_rows = []

                sql_rows.append(sql_row)
                sql_size = sql_size + len(sql_row) + 1

            if len(sql_rows) > 0:
                sql = sql_header + ",".join(sql_rows) + sql_suffix
                print("inserting " + str(len(sql_rows)) + " rows (" + str(len(sql)) + " bytes) ... ", end='')
                execute(sql)
                print('OK')

            if conn is not None:
                conn.commit()

        except:
            # roll back in case of exception but ignore other exceptions happining
            # TODO: don't ignore all other exceptions
            try:
                if conn is not None and use_transaction:
                    conn.rollback()
            except:
                pass

            raise

        finally:
            # finally close the cursor and connection
            if conn is not None:
                cursor.close()
                conn.close()

