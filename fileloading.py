    import os
    import logging
    from pyspark.sql import SparkSession

    class csvloader:
        def __init__(self,spark_session,file_path,options=None):
            self.spark = spark_session
            self.file_path= file_path
            self.options = options

        def load(self):
            try:
            df = self.spark.read.options(**self.options).csv(self.file_path)
            logging.info(f"CSV file '{self.file_path}' loaded successfully.")
            return df
            except Exception as e:
            logging.error(f"Failed to load CSV file '{self.file_path}': {e}")
            raise 
        
        def setup_spark(app_name="CSVLoaderApp"):
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        logging.info("Spark session started.")
        return spark

        def main():
            logging.basicConfig(level=logging.info,format="%(asctime)s - %(levelname)s - %(message)s")
            csv_file_path = "/Users/jairamsujithyarram/data/input/tsunami.csv"
            csv_options = {
                "header": "true",
                "inferSchema": "true",
                "delimiter": ","
            }
        spark = setup_spark()

        # Load CSV
        loader = CSVLoader(spark, csv_file_path, csv_options)
        dataframe = loader.load()

        # Show data preview
        dataframe.show(5)

        # Stop Spark session
        spark.stop()
        logging.info("Spark session stopped.")

    if __name__ == "__main__":
        main()