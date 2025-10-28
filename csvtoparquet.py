import os
import findspark
import smtplib
from email.mime.text import MIMEText
from pyspark.sql import SparkSession

# --- Configuration ---
csv_file_path = "/Users/jairamsujithyarram/data/input/Traffic.csv"
parquet_output_path = "/Users/jairamsujithyarram/data/output"
email_recipient = "sujithyarram@gmail.com"
email_sender = "jairamsujith36@gmail.com"
email_password = "seee aans kixf erft"  # Use an app password for security

smtp_server = "smtp.gmail.com"
smtp_port = 587  # Common for TLS

def send_email(subject, body):
    """Send an email using smtplib and MIMEText."""
    try:
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = email_sender
        msg['To'] = email_recipient

        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(email_sender, email_password)
            server.sendmail(email_sender, [email_recipient], msg.as_string())
        print("Email notification sent successfully.")
    except Exception as e:
        print(f"Failed to send email: {e}")

def process_csv_to_parquet():
    try:
        # Setup environment for PySpark
        os.environ['JAVA_HOME'] = '/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home'
        os.environ['SPARK_HOME'] = '/opt/homebrew/opt/apache-spark/libexec'
        findspark.init()

        spark = SparkSession.builder.appName("CsvToParquetConverter").getOrCreate()
        print("SparkSession created.")

        # Read CSV data with header and infer schema
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_file_path)

        if df.rdd.isEmpty():
            message_body = f"Alert: The CSV file '{csv_file_path}' was empty. No data processed."
            print(message_body)
            send_email("PySpark Job Failed: No Data Received", message_body)
        else:
            # Write dataframe as Parquet
            df.coalesce(1).write.mode("overwrite").parquet(parquet_output_path)
            message_body = f"Success: The CSV file '{csv_file_path}' was successfully converted to Parquet.\nOutput saved at '{parquet_output_path}'."
            print(message_body)
            send_email("PySpark Job Success: Data Converted", message_body)

    except Exception as e:
        error_message = f"An error occurred during processing: {e}"
        print(error_message)
        send_email("PySpark Job Failed: Processing Error", error_message)
    finally:
        if 'spark' in locals():
            spark.stop()
            print("SparkSession stopped.")

if __name__ == "__main__":
    process_csv_to_parquet()
