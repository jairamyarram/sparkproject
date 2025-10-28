import os
import findspark
import smtplib
from email.mime.text import MIMEText
from pyspark.sql import SparkSession

# --- Configuration ---
json_file_path = "data/data.json"
csv_output_path = "output1/output.csv"
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

def process_json_to_csv():
    """Reads a JSON file, converts it to CSV, and sends email notifications."""
    try:
        # --- Environment Setup for PySpark ---
        os.environ['JAVA_HOME'] = '/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home'
        os.environ['SPARK_HOME'] = '/opt/homebrew/opt/apache-spark/libexec'
        findspark.init()

        # --- Create Spark Session ---
        spark = SparkSession.builder.appName("JsonToCsvConverter").getOrCreate()
        print("SparkSession created.")

        # --- Read JSON data ---
        df = spark.read.option("multiline", "true").json(json_file_path)

        # --- Check for empty data ---
        if df.rdd.isEmpty():
            message_body = f"Alert: The file '{json_file_path}' was empty. No data was processed or converted to CSV."
            print(message_body)
            send_email("PySpark Job Failed: No Data Received", message_body)
        else:
            # --- Convert to CSV and save ---
            df.coalesce(1).write.csv(csv_output_path, header=True, mode="overwrite")
            message_body = f"Success: The file '{json_file_path}' was successfully processed and converted to CSV.\n" \
                           f"The output is available at '{csv_output_path}'."
            print(message_body)
            send_email("PySpark Job Success: Data Processed", message_body)

    except Exception as e:
        error_message = f"An error occurred during PySpark processing: {e}"
        print(error_message)
        send_email("PySpark Job Failed: Processing Error", error_message)
    finally:
        if 'spark' in locals():
            spark.stop()
            print("SparkSession stopped.")

# --- Execute the function ---
if __name__ == "__main__":
    process_json_to_csv()
