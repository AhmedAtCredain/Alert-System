import pandas as pd
import csv  
import datetime
from lambda_function import *
import boto3
from io import StringIO
import json
import os

##########################
def read_s3_file_into_df(s3, s3_bucket_mame, file):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=s3_bucket_mame, Key=file)
    df = pd.read_csv(obj['Body'])
    return df

def write_df_to_s3_file(s3, s3_bucket_mame, filename, df_File):
    csv_buffer = StringIO()
    df_File.to_csv(csv_buffer, index=False)
    response = s3.Object(s3_bucket_mame, filename).put(
        Body=csv_buffer.getvalue())
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(
            f"Successful S3 upload response. Status - {status} - output created")
    else:
        print(
            f"Unsuccessful S3 upload response. Status - {status} - OUTPUT Not passed")

def write_dict_to_s3_file(s3, s3_bucket_mame, filename, response_dict):
    json_string = json.dumps(response_dict)
    response = s3.Object(s3_bucket_mame, filename).put(
        Body=json_string)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(
            f"Successful S3 upload response. Status - {status} - output created")
    else:
        print(
            f"Unsuccessful S3 upload response. Status - {status} - OUTPUT Not passed")

def upload_file_to_s3_archive(s3, s3_bucket_mame, file_to_upload, filename):
    csv_buffer = StringIO()
    file_to_upload.to_csv(csv_buffer, index=False)
    filenameX = os.path.splitext(filename)[0]

    now = datetime.now()
    dttm = now.strftime("%Y_%m_%d_%H_%M_%S")
    key = "DataValidation/" + filenameX + "_" + dttm + "_" + ".csv"

    response = s3.Object(s3_bucket_mame, key).put(
        Body=csv_buffer.getvalue())
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(
            f"Successful S3 upload response. Status - {status} - archive created-Filename- {filename}")
    else:
        print(
            f"Unsuccessful S3 upload response. Status - {status} - archive not created-Filename -{filename}")

def delete_s3_file(s3, s3_bucket_mame, filename):
    response = s3.Object(s3_bucket_mame, filename).delete()
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 204:
        print(
            f"Successful S3 Delete response. Status - {status} - Filename - {filename}")
    else:
        print(
            f"Unsuccessful S3 Delete response. Status - {status} - Filename - {filename}")


def upload_file_rejected(s3, s3_bucket_mame, file_to_upload, filename):
    csv_buffer = StringIO()
    file_to_upload.to_csv(csv_buffer, index=False)

    now = datetime.now()
    dttm = now.strftime("%Y_%m_%d_%H_%M_%S")
    key = dttm + "_" + filename

    response = s3.Object(s3_bucket_mame, key).put(
        Body=csv_buffer.getvalue())
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(
            f"Successful S3 upload response. Status - {status} - rejected file sent")
    else:
        print(
            f"Unsuccessful S3 upload response. Status - {status} - rejected file not sent")


##########################
# S3 Varaibles
s3 = boto3.resource('s3')
s3_client = boto3.client("s3")

s3_subslist_master_bucket = 'kala.dev.subs-list-master'
filename_subslist = 'MasterData.csv'

s3_subslist_error_bucket = 'kala.dev.alert-log'
filename_error = 'Alert.csv'
##########################

def set_alert(data):
    # offline
    # with open('Errors.csv', 'a+', newline='',encoding='UTF8') as f:
    #     writer = csv.writer(f)
    #     writer.writerow(data)
    # AWS

    error_df=read_s3_file_into_df(s3, s3_subslist_error_bucket, filename_error)
    error_df=error_df.append(pd.DataFrame([data],columns=error_df.columns))
    write_df_to_s3_file(s3, s3_subslist_error_bucket, filename_error, error_df)



class ValidationCheck():

    def __init__(self,array):
        data={
            
            'SytemDataCaptureDate':array[1],
            'TransactionDate':array[2],
            'PaymentDueDate':array[3],
            'DataSender':array[4],
            'Payer':array[5],
            'Payee':array[6],
            'Amount':array[7],
            'CCY':array[8],
            'TxnCCY':array[9],
            'PaymentBankAccountId':array[10],
            'PaymentBankName':array[11],
            'PaymentBankAcctCCY':array[12]

        }
        self.invoice_number = array[0]
        self.details = data
    
   
    def check_mismatched_subsidiary(self):
        
        # master_data = pd.read_csv('MasterData.csv').iloc[:,0].values
        
        master_data = read_s3_file_into_df(s3, s3_subslist_master_bucket, filename_subslist).iloc[:,0].values
        payee = self.details['Payee']
        payer = self.details['Payer']
        data_sender = self.details["DataSender"]
        data=[
                'Mismatched Subsidiary',
                self.invoice_number,
                self.details,
                "high",
                "unresolved"
            ]

        if payee not in master_data:
            data[0]='Mismatched Subsidiary (Payee)'
            set_alert(data)
        
        if payer not in master_data:
            data[0]='Mismatched Subsidiary (Payer)'
            set_alert(data)

        if data_sender not in master_data:
            data[0]='Mismatched Subsidiary (Data Sender)'
            set_alert(data)
            
    
    def check_amount(self):
        high=200000.0
        amount=float(self.details["Amount"])
        
        if amount>high:
            data=[
                    'Unusual Amount (Higher than expected)',
                    self.invoice_number,
                    self.details,
                    "mid",
                    "unresolved"
                ]
            set_alert(data)

    
    def checkall(self):
        self.check_amount()
        self.check_mismatched_subsidiary()

        
class ReconciliationCheck():
    def __init__(self,array):
        data={
            
            'SytemDataCaptureDate':array[1],
            'TransactionDate':array[2],
            'PaymentDueDate':array[3],
            'DataSender':array[4],
            'Payer':array[5],
            'Payee':array[6],
            'Amount':array[7],
            'CCY':array[8],
            'TxnCCY':array[9],
            'PaymentBankAccountId':array[10],
            'PaymentBankName':array[11],
            'PaymentBankAcctCCY':array[12]

        }
        self.invoice_number = array[0]
        self.details = data
    
   
    def check_reconciliation_date (self):
        transaction_due_date=self.details['PaymentDueDate']
        due_date=(datetime.date.today()+datetime.timedelta(days=2)).strftime("%m/%d/%Y")
        if transaction_due_date==due_date:
            data=[
                    'Transaction Due in 2 Days',
                    self.invoice_number,
                    self.details,
                    "low",
                    "unresolved"
                ]
            set_alert(data)

    def checkall(self):
        self.check_reconciliation_date()
        

# errors=[

# ['C26','01/16/2022','01/14/2022','03/18/2023','Ahmed','Green Kaya Limited','Super Pharma ANZ Private Limited','293253','USD','USD','','',''],
# ['C27','01/16/2022','01/14/2022','02/28/2022','Super Pharma ANZ Private Limited','Ahmed','Super Pharma ANZ Private Limited','193253','USD','USD','','',''],
# ['C28','01/16/2022','01/14/2022','02/28/2022','Super Pharma ANZ Private Limited','Green Kaya Limited','Ahmed','193253','USD','USD','','',''],
# ['C50','01/16/2022','01/14/2022','02/13/2022','Super Pharma ANZ Private Limited','Tiositiz Public Joint Stock Company','Super Pharma ANZ Private Limited','375876','USD','USD','','','']

# ]

# for i in errors:
#     obj=ValidationCheck(i)
#     obj.checkall()
#     obj1=ReconciliationCheck(i)
#     obj1.checkall()
