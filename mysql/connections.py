from abc import ABCMeta,abstractmethod

from pyspark import SQLContext
from pyspark import SparkContext
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DoubleType
from pyspark.sql import Row

class Connection(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self):
        pass

class FileConnection(Connection):
    def __init__(self,spark_context,file_path):
        self.file_path=file_path
        self.spark_context=spark_context
        self.rdd=None
        self.delimiter='\t'
        self.sql_context = SQLContext(self.spark_context)
        self.header=None

    def read_file_to_rdd(self):
        # self.rdd = self.spark_context.textFile(self.file_path)
        pass



    def rdd_to_df(self):
        def generate_tuple(line):
            return tuple(line)

        def get_delimiter():
            #return self.delimiter
            return '\t'

        # Split lines to structured lines
        self.rdd = self.spark_context.textFile(self.file_path)
        #delimit=self.delimit
        self.delimit='\t'
        header_record = self.rdd.first()
        print (header_record)
        rows = self.rdd.filter(lambda line: line != header_record)
        delimited_rows = rows.map(lambda line: line.split(get_delimiter()))

        # Convert structured lines to tuple
        #print rows.count()
        tuples = delimited_rows.map(generate_tuple)


        ## Associate tuples to schema
        #  Generate schema
        if self.header is True:
            schema = self.generate_schema(' '.join(self.header_string.split(self.delimiter)))
        return self.sql_context.createDataFrame(
                                data=tuples,
                                schema=schema,
                                verifySchema=True)

    def generate_schema(self,schema_string=None):
        if schema_string is None:
            return Exception("Schema required")

        fields = [StructField(field_name, StringType(), True)
                  for field_name in schema_string.split()]
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

        self.sqlContext = SQLContext(spark_context)
        self.dataframe = self.sqlContext.read.format("jdbc")\
                .option("url","jdbc:mysql://"+server+":3306/"+db_name)\
                .option("driver","com.mysql.jdbc.Driver")\
                .option("user",username)\
                .option("password",password) \
                .option("dbtable",query) \
                .load()

    def get_dataframe(self):
        return self.dataframe


