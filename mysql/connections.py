from abc import ABCMeta,abstractmethod

from pyspark import SQLContext
from pyspark import SparkContext

class Connection(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self):
        pass

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


