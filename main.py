import boto3
import os
import csv
import configparser
from datetime import date
import json
from validations.city_validation import validate_city
from validations.fields_validation import validate_field
from validations.order_date_validation import validate_order_date
from validations.product_validation import validate_product
from validations.sales_validation import validate_sales
from send_mail import send_mail

config = configparser.ConfigParser()
config.read('config.ini')

aws_key = config['aws']['access_key']
aws_secret = config['aws']['secret_key']

with open('config.json', 'r') as f:
    config = json.load(f)
    
today = date.today().strftime("%Y%m%d")
bucket_name = config['s3_settings']["bucket_name"]
s3_source_file_path = config["s3_settings"]["prefixes"]["incoming_files"]+ today
s3_success_file_path = config["s3_settings"]["prefixes"]["success_files"]+ today
s3_reject_file_path = config["s3_settings"]["prefixes"]["rejected_files"]+ today


s3= boto3.client(
"s3",
aws_access_key_id= aws_key,
aws_secret_access_key= aws_secret
)

c_date = date.today().strftime("%Y-%m-%d")
success_count = 0
failed_count = 0

product_master = {}

response = s3.list_objects_v2(Bucket = bucket_name,Prefix=s3_source_file_path)    
for obj in response.get("Contents", []):
    file_key = obj["Key"]

    if file_key.split("/")[-1] == "product_master.csv" :
        
        filename = file_key.split("/")[-1]
        compl_source_path = s3_source_file_path + "/"+ filename
        s3.download_file(bucket_name, compl_source_path, filename)
        
        with open(filename, "r") as file:
            reader = csv.DictReader(file)
            for row in reader:
                product_master[row['product_id']] = row['price']
                    
        if os.path.exists(filename):
            os.remove(filename)
            
for obj in response.get("Contents", []):
    file_key = obj["Key"]

    if file_key.split("/")[-1].startswith("orders"):
        filename = file_key.split("/")[-1]
        compl_source_path = s3_source_file_path + "/"+ filename
        print(f"\n🔍 Processing file: {filename}")
        obj = s3.get_object(Bucket=bucket_name, Key=compl_source_path)
        file_content = obj["Body"].read().decode("utf-8")
        lines = file_content.strip().split('\n')
        is_success = "True"
        validation_failed_rows = []
        validation_failed_rows.append(lines[0].strip()+',rejection_reason')
            
        for line in lines[1:]:
            product_id = line.split(",")[2]
            sales_validation = validate_sales(product_master,line)
            city_validation = validate_city(line)
            field_validation = validate_field(line)
            order_date_validation = validate_order_date(line,date.today())
            product_validation = validate_product(product_master , product_id )

            valid_city = "True"
            valid_field = "True"
            valid_order = "True"  
            valid_product = "True"
            valid_sales = "True"
            error_messages = []
            
            if city_validation :
                valid_city = "False"
                error_messages.append("city is invalid")
            if field_validation :
                valid_field = "False"
                error_messages.append("field is invalid")
            if order_date_validation :
                valid_order = "False"
                error_messages.append("order_date is invalid")
            if product_validation :
                valid_product = "False"
                error_messages.append("product id is invalid")
            if sales_validation :
                valid_sales = "False"
                error_messages.append("Sales is incorrect")
                     
            if valid_city == "False" or  valid_field == "False" or  valid_order == "False" or valid_product == "False" or valid_sales=="False":
                validation_failed_rows.append(line.strip() + "," + " ; ".join(error_messages))
            
        if len(validation_failed_rows) == 1 :
            print("All validations are checked and passed")
            success_count += 1
            copy_source = {'Bucket': bucket_name, 'Key': compl_source_path }
            s3.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=s3_success_file_path + "/" + filename)
            s3.delete_object(Bucket=bucket_name, Key=compl_source_path)
        else :
            is_success = "False"
            print("All validations are checked and failed")
            failed_count += 1
            copy_source = {'Bucket': bucket_name, 'Key': compl_source_path }
            s3.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=s3_reject_file_path + "/" + filename)
            s3.delete_object(Bucket=bucket_name, Key=compl_source_path)

        with open(f"error_{filename}", "w" ,newline="") as file:
            writer = csv.writer(file)
            for row in validation_failed_rows :
                writer.writerow(row.strip().split(","))
        s3.upload_file(f"error_{filename}",bucket_name , s3_reject_file_path + "/" + f"error_{filename}")
        
        if os.path.exists(f"error_{filename}"):
            os.remove(f"error_{filename}")
        

if   success_count == 0 and  failed_count == 0:   
    subject = f" validation email - {c_date} " 
    body = f"No incoming files found for {c_date}"
    send_mail(subject,body)
else :
    subject = f" validation email - {c_date} " 
    body = f"Total  { success_count + failed_count } files processed.\n {success_count} passed validation. \n {failed_count} files failed validation."
    send_mail(subject,body)