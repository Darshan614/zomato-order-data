class Log4j():

    def __init__(self,spark):
        conf = spark.sparkContext.getConf()
        app_id = conf.get('spark.app.id')
        app_name = conf.get('spark.app.name')

        log4j = spark._jvm.org.apache.log4j
        message_prefix = '<' + app_id + ' ' + app_name + '>'
        self.logger = log4j.LogManager.getLogger(message_prefix)

    def error(self,message):
        self.logger.error(message)

    def warn(self,message):
        self.logger.warn(message)

    def info(self,message):
        self.logger.info(message)