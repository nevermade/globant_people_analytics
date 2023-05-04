# Description

This python project provides an API to run some ingestion data procceses. This project includes additional features like the following:

1.  Upload CSV for 3 tables:
    * hired_employees
    * departments
    * jobs
2. Backup the tables from snowflake to AVRO files.
3. Restore tables from AVRO files

## Considerations

* I am using snowflake as my database since it provides a free tier for testing.

* The rest api for uploading data will ask the user to provide a csv file. This file will be uploaded and then it will serve to create the table on Snowflake.

* I am using a config file to get my snowflake credentials. This will be provided on the email that I will send to Giovanna and Carlos.

# API Endpoints

## How to use it

To execute the upload process on the server you need to call one of thefollowing endpoints:

`http://127.0.0.1:3000/upload_data/<entity>`

`http://127.0.0.1:3000/backup/<entity>`

`http://127.0.0.1:3000/restore/<entity>`

`<entity>` stands for the tables that need to be proccessed. Allowed values are:

* hired_employees
* departments
* jobs


For the upload process, the server will return a list of rows that were not inserted because it didn't fulfill provided rules. These rows will be
provided as an HTML table.

For the backup process, server will return the avro file of the provided entity on the URL string

For the restore process, server will ask the user to provide an avro file. This file will be used for restoring the table based on the provided entity.


# How to run the project

1. Clone this repo.
3. Run the following command to build the docker image:
```bash
docker build -t globant_people_analytics .
```
4. Run the server with the following command:
```bash
docker run -p 3000:3000 globant_people_analytics
```
5. Start using the app on your localhost with the described endpoints.