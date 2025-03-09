import os
from wsgiref import validate 
from numpy import subtract
import pandas as pd 
import logging
import smtplib
import schedule
import time
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# configuring  logging
logging.basicConfig(
    filename="Ecommerce_data_cleaning.log",
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Email configuring 

# Manually setting environment variables in the script
os.environ["EMAIL_SENDER"] = "cyrila@outlook.com"
os.environ["EMAIL_PASSWORD"] = "NIWCJNJJVFN!"
os.environ["EMAIL_RECEIVER"] = "cyrila@outlook.com"

EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")
SMTP_SERVER = "smtp.office365.com"
SMTP_PORT = 587


# Debugging output to check values
print(f"EMAIL_SENDER: {EMAIL_SENDER}")
print(f"EMAIL_PASSWORD: {EMAIL_PASSWORD}")
print(f"EMAIL_RECEIVER: {EMAIL_RECEIVER}")

# Ensuring credentials are set 
if not EMAIL_SENDER or not EMAIL_PASSWORD or not EMAIL_RECEIVER:
    logging.error("Please set email sender and password, and email reciever")
    raise ValueError("Please set email sender and password, and email reciever")


def send_email(sender, message):
    """Sends an email notification"""
    msg = MIMEMultipart()
    msg['From'] = EMAIL_SENDER
    msg['To'] = EMAIL_RECEIVER
    msg['Subject'] = "Data Processing Notification"
    msg.attach(MIMEText(message, 'plain'))

    try: 
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())
        server.quit()
        logging.info(" Email notification sent successfuly!!")
    except Exception as e: 
        logging.error(" Failed to send email: {e}")


def load_and_clean_data(data_folder):
    """Loading data, valadting, and logging all CSV files in a given folder."""
    dataframes = {}
    
    for files in os.listdir(data_folder):
        if files.endswith(".csv"):
            file_path = os.path.join(data_folder, files)
            try:
                    # trying to read file with utf-8 encoding 
                    df = pd.read_csv(file_path, encoding='utf-8')
                    logging.info(f"successfully loaded {files}")
            except UnicodeDecodeError:
                    logging.warning(f"Unicode error in {files}, trying ISO-8859-1....")
                    try:
                        df = pd.read_csv(file_path, encoding='ISO-8859-1')
                        logging.info(f"sucessfully loaded {files}, trying ISO-8859-1 encoding")
                    except Exception as e:
                        logging.error(f"failed to load {files}: {e}")
                        continue

                # adding the loaded dataframe to dict
            dataframes[files] = df
            
    return dataframes
        
    
def clean_data(df):
    """ Cleaning the dataframe by removing Nan values and duplicates """
    try: 
        # removing rows with Nan values
        df.dropna(inplace = True)
        logging.info("removed Nan Values")

        # removing duplicates rows 
        df.drop_duplicates(inplace=True)
        logging.info("dropped all duplicates rows")

        return df
    except Exception as e:
        logging.error(f"data cleaning failed: {e}")
        send_email("data cleaning failed", f"data cleaning failed due to erro: {e}")
        return None
    
    
def fix_data_types(df):
    """ Automatically fix data types of columns based on the content """
    try:
        for column in df.columns:
            # converts numeric columns to the right types
            if df[column].dtypes == 'object':
                # then attempts to converts to numeric
                try:
                    df[column] = pd.to_numeric(df[column])
                except ValueError:
                    pass
                

            # converts date columns if it exits
            if df[column].dtype == 'object' and any(char.isdigit() for char in df[column].head(10)):
                try:
                    df[column] = pd.to_datetime(df[column], errors='coerce')
                    logging.info(f"converted column {column} to datetime")
                except Exception as e:
                    logging.error(f" data type fixing failed: {e}")
        
        return df
    except Exception as e:
        logging.error(f" data type fixing failed: {e}")
        send_email("data type fixing failed ", f" data type fixing faled: {e}")
        return None
    
    
def standardize_text(df):
    """ Standardizing text columns to lowercase and strips extra space """
    try:
        # looping through the column
        for column in df.select_dtypes(include=['object']).columns:
            # converts lowercases and strip off extra spaces
            df[column] = df[column].str.lower().str.strip()
        logging.info("standardize text columns")
        return df
    except Exception as e:
        logging.error(f"standardizing text failed: {e}")
        send_email("standardizing text failed", f"standardizing text failed due to: {e}")
        return None
    
def validate_data(df):
    """ Performing basic data valiadation: checking for missing values and duplicates """
    try:
        # checking misisng values 
        missing_values = df.isnull().sum()
        if missing_values.any():
            logging.warning(f" missing values found: {missing_values}")

        # checking for duplicates 
        duplicate_values = df.duplicated().sum()
        if duplicate_values > 0:
            logging.warning(f" duplicates values found: {duplicate_values}")

        return True
    except Exception as e:
        logging.warning(f"data validation failed: {e}")
        send_email("data validation failed", f"data validation failed due to: {e}")
        return None
    


  
def load_and_cleaned_data(data_folder, cleaned_path):
    """ Loads, cleans, valaidates, and saves the cleaned dataframe to all csv files in a given folder """
    cleaned_dataframes = {}

    # loading the data
    dataframes = load_and_clean_data(data_folder)

    # cleaning and valdating each dataframe
    for filename, df in dataframes.items():
        # cleaning the dataframe
        df_cleanded = clean_data(df)
        if df_cleanded is not None:
            df_cleanded = fix_data_types(df_cleanded)
            df_cleanded = standardize_text(df_cleanded)

            # validating the cleaned dataframe
            if validate_data(df_cleanded):
                cleaned_dataframes[filename] = df_cleanded

                # saving the cleaned dataframe
                cleaned_file_path = os.path.join(data_folder, f"cleaned_{filename}")
                df_cleanded.to_csv(cleaned_file_path, index=False)
                logging.info(f"saved cleaned data as {cleaned_file_path}")
            else:
                logging.warning(f"{filename} failed vaildation. Not saved")
        else:
            logging.warning(f"{filename} failed cleaning. Not saved")

        # sends sucessful email notification
        send_email(f"data processing  completed", f"Data processing for all csv files in {data_folder} is completed ")
        return cleaned_dataframes


import functools

def schedule_data_processing(data_folder, cleaned_path, interval_minutes=60):
    """ Schedules the data processing task to run periodically """
    logging.info(f"Scheduling data processing every {interval_minutes} mins")

    # Use functools.partial to pass multiple arguments to the scheduled function
    job = functools.partial(load_and_cleaned_data, data_folder, cleaned_path)
    schedule.every(interval_minutes).minutes.do(job)

    while True:
        schedule.run_pending()
        time.sleep(30)



if __name__ == "__main__":
    data_folder = r"C:\Users\cyril\Downloads\dataDownloads\ECommerce Data Analysis"

    #data_folder = r"C:\Users\cyril\Downloads\dataDownloads\ECommerce Data Analysis"
    cleaned_path = r"C:\Users\cyril\Downloads\dataDownaloads\CleanedData"

    schedule_data_processing(data_folder, cleaned_path)



                   
                    