from abc import ABCMeta,abstractmethod

from pyspark import SQLContext
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType

from datetime import datetime

class Connection(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self):
        pass

class FileConnection(Connection):
    def __init__(self,spark_context,sql_context,file_path):
        self.file_path=file_path
        self.spark_context=spark_context
        self.rdd=None
        self.delimiter='\t'
        self.sql_context = sql_context
        self.header=None
        self.header_string = None

    def read_file_to_rdd(self):
        self.rdd = self.spark_context.textFile(self.file_path)


    def rdd_to_df(self):
        def generate_tuple(line):
            return tuple(line)

        def get_delimiter():
            #return self.delimiter
            return '\t'

        # If header record in file, use that else use the parameter passed
        if self.header:
            header_record = self.rdd.first()
        else:
            header_record = self.header_string

        # Split lines to structured lines
        rows = self.rdd.filter(lambda line: line != header_record)
        delimited_rows = rows.map(lambda line: line.split(get_delimiter()))

        # Convert structured lines to tuple
        tuples = delimited_rows.map(generate_tuple)


        ## Associate tuples to schema
        #  Generate schema
        schema = self.generate_schema('\t'.join(header_record.split(get_delimiter())))
        print schema
        return self.sql_context.createDataFrame(
                                data=tuples,
                                schema=schema,
                                verifySchema=True)

    def generate_schema(self,schema_string=None):
        if schema_string is None:
            return Exception("Schema required")

        fields = [StructField(field_name, StringType(), True)
                  for field_name in schema_string.split('\t')]
        schema = StructType(fields)
        return schema


    def read_into_df(self,delimiter=None,header=False,header_string=None):
        if delimiter is None:
            raise Exception ("Delimiter required to read file")
        self.delimiter = delimiter
        self.header=header
        self.header_string=header_string

        self.read_file_to_rdd()
        self.data_frame = self.rdd_to_df()
        return self.data_frame


class DatabaseConnection(Connection):

    def __init__(self,
                 server=None,
                 username=None,
                 password=None,
                 db_name=None,
                 port=None
                 ):
        self.server = server
        self.username = username
        self.password = password
        self.db_name = db_name

class TCPSocketConnection (Connection):
    def __init__(self,hostname,port):
        self.hostname = hostname
        self.port = port

class SparkStreamingTCPSocketConnection (TCPSocketConnection):
    def __init__(self,spark_context,hostname,port,batch_interval=2):

        try:
            from pyspark.streaming import StreamingContext
        except Exception as e:
            raise Exception("Install pyspark",e)

        super(SparkStreamingTCPSocketConnection,self).__init__(
            hostname=hostname,
            port=port
        )
        self.spark_context = spark_context
        self.streaming_context=StreamingContext(self.spark_context,
                                                batch_interval)
        self.dstreams = self.streaming_context.socketTextStream\
            (self.hostname,
             self.port)


    def ingest(self):
        # Function to map the point to the right quadrant
        def words_from_lines(line):
            print (line)
            # Convert the input string into a pair of numbers
            return (line,1)


        # Function that's used to update the state
        self.streaming_context.checkpoint("checkpoint")

        updateFunction = lambda new_values, running_count: sum(new_values) + (running_count or 0)

        # Update all the current counts of number of points in each quadrant
        running_counts = self.dstreams.map(words_from_lines).\
                         updateStateByKey(updateFunction)

        # Print the current state
        running_counts.saveAsTextFiles("/home/senthil/code/pyspark/fraud/data/processed/")

        # Start the computation
        self.streaming_context.start()

        # Wait for the computation to terminate
        self.streaming_context.awaitTermination()


class MySQLConnector(DatabaseConnection):
    CONNECT_TIMEOUT=5

    def __init__(self,
                 server=None,
                 username=None,
                 password=None,
                 db_name=None,

                 ):

        try:
            import MySQLdb as mysql_connector
        except Exception:
            raise Exception("Install MySQLdb client")

        super(MySQLConnector,self).__init__(
            server=server,
            username=username,
            password=password,
            db_name=db_name,
        )

        try:
            self.connection_obj=mysql_connector.connect(
                host=server,
                user=username,
                passwd=password,
                db=db_name,
                connect_timeout=self.CONNECT_TIMEOUT
            )
        except Exception as e:
            raise Exception("Failed to connect :",e)



class SparkMySQLConnector(DatabaseConnection):
    CONNECT_TIMEOUT=5

    def __init__(self,
                 spark_context,
                 server=None,
                 username=None,
                 password=None,
                 db_name=None,
                 query=None

                 ):
        if spark_context is None:
            return False

        super(SparkMySQLConnector,self).__init__(
            server=server,
            username=username,
            password=password,
            db_name=db_name,
        )

        self.sqlContext = SQLContext(spark_context)
        self.dataframe = self.sqlContext.read.format("jdbc")\
                .option("url","jdbc:mysql://"+server+":3306/"+db_name)\
                .option("driver","com.mysql.jdbc.Driver")\
                .option("user",username)\
                .option("password",password) \
                .option("dbtable",query) \
                .load()
        print self.dataframe.count()

    def get_dataframe(self):
        return self.dataframe

    def write_df_into_table(self,data_frame=None,table_name=None):
        if data_frame and table_name:
            properties = {
                "driver":"com.mysql.jdbc.Driver",
                "user":self.username,
                "password":self.password,
                    }
            print ("Writing...")

            try:
                print self.server,self.db_name,table_name,self.username,self.password
                print datetime.now()
                data_frame.write.format("jdbc") \
                    .option("url","jdbc:mysql://"+self.server+":3306/"+self.db_name) \
                    .option("driver","com.mysql.jdbc.Driver") \
                    .option("user",self.username) \
                    .option("password",self.password) \
                    .option("dbtable",table_name) \
                    .option("rewriteBatchedStatements","true") \
                    .mode("append")\
                    .save()
                print datetime.now()
            except Exception as excp:
                raise Exception("Failure to write", excp)
        else:
            raise Exception("Dataframe can't be NULL")


