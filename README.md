# Data Engineering Zoomcamp Course 2024
# Project: GDELT Project Events workflow and Visualization

Repo Folder for tasks and homeworks included in the Module 3: Data Engineering Zoomcamp course Cohort 2024.

## Content of the Module

1. Data Warehouse
2. BigQuery
3. Partitioning and clustering
4. BigQuery best practices
5. Internals of BigQuery
6. BigQuery Machine Learning

## Instruction to prepare GCP
1. Create a GCP Project 

Project name
GDELT-Project-Data
Project number
297075219918
Project ID
gdelt-project-data

2. Create a service account in the project, permissions:
	Storage Admin
	Bigquery Admin
	NO: Service Management Administrator
	Service Usage Admin
3. Create an access key
	SAve the json file to your mage directory (DEFINE)

To run the project
1. Clone the repo
2. Create the objects with Terraform
	Next, modify variables.tf f you want to change the names of the GCP object to create
	1. TErraform init
	2. TErraform plan
		Check the changes or modification to execute
	3. terraform apply
		To apply the changes, GCP objects will be created in the account
	4. terraform destroy
		Important: When you finished testing this project

3. Go to mage directory
4. Build the container
	docker compose build
5. Run the contaier
	docker compose uo




