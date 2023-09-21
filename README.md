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
Example result of the command. Note a key `storage_bucket_name`: 

![](docs/terraform_apply_results.png)

* Clone this repo into your Databricks workspace.
* Main - and only one - notebook is located in `notebooks/Spark SQL Homework.sql` file.
* Put the name of the created bucket into a third cell, preserving a following format: `gs://<YOUR_BUCKET_NAME>`. 
This key is what should be put here in the notebook:

![](docs/saving_bucket_name.png)

* And you're ready to go! Run cell one by one.

