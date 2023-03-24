import pandas as pd
from Alert import *

errors=pd.read_csv("Errors.csv")

def writer_function(name,data):
    if name=="Errors.csv":
        with open(name, 'w+', newline='',encoding='UTF8') as f:
            writer = csv.writer(f)
            writer.writerow(['error_name', 'related_invoice', 'error_details', 'priority', 'status'])
            for d in data:
                writer.writerow(d)

    elif name=="Transactions.csv":
        with open(name, 'w+', newline='',encoding='UTF8') as f:
            writer = csv.writer(f)
            writer.writerow(['InvoiceNumber','SytemDataCaptureDate','TransactionDate','PaymentDueDate','DataSender','Payer','Payee','Amount','CCY','TxnCCY','PaymentBankAccountId','PaymentBankName','PaymentBankAcctCCY'])
            for d in data:
                writer.writerow(d)




def update_data(data):
    invoice_number=data[0]
    error_index_value=list(errors.iloc[:,1].values).index(invoice_number)
    errors_values=errors.values
    del errors_values[error_index_value]
    transactions=pd.read_csv("Transactions.csv")
    transactions_values=transactions.values
    transactions_index_value=list(errors.iloc[:,0].values).index(invoice_number)
    transactions_values[transactions_index_value]=data

    


transactions=[

['C26','01/16/2022','01/14/2022','03/18/2023','Ahmed','Green Kaya Limited','Super Pharma ANZ Private Limited','293253','USD','USD','','',''],
]

for i in transactions:
    update_data(i)
   
