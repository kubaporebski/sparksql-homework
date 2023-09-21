# Spark SQL Homework

## How to run in Databricks?
* Make sure you run terraform first:
```
cd terraform
terraform init
terraform plan -out terraform.plan
terraform apply terraform.plan
```
Results of the last command should display, among other things, a google cloud bucket name. This will be required to run the notebook.
* Clone this repo into your Databricks workspace.
* Main - and only one - notebook is located in `notebooks/Spark SQL Homework.sql` file.
* Put the name of the created bucket into a third cell, preserving format: `gs://<YOUR_BUCKET_NAME>`
* And you're ready to go! Run cell one by one

