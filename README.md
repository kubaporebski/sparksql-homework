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
Following image shows the result of the command. Note a key `storage_bucket_name`: 

![](docs/terraform_apply_results.png)

* Clone this repo into your Databricks workspace.
* Main - and only one - notebook is located in `notebooks/Spark SQL Homework.sql` file.
* Put the name of the created bucket into a third cell, preserving a following format: `gs://<YOUR_BUCKET_NAME>`. 
This key is what should be put here in the notebook:

![](docs/saving_bucket_name.png)

* Now, upload the data from the module homework page. These are data files which you should unpack:

![](docs/material_homework.png)

As you can see, there are two folders inside the ZIP file, which look like this after unpacking:

![](docs/material_structure.png)

You should upload folders one by one in a such way that each uploaded folder will be located in the root folder of the bucket:

![](docs/input_data.png)

I did it, but you can omit uploading files: `.DS_Store` and `DataDescription.md`.

* And you're ready to go! Open the notebook inside Databricks Workspace, and then run cell one by one.

