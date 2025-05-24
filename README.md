# Project Title

## Overview

This project is a production-level data engineering solution designed to process and transform large volumes of data. It focuses on extracting data from AWS S3 buckets, validating the data locally, and loading it into a data mart or SQL database.

## Features

* **Data Extraction from AWS S3:** Utilizes the AWS SDK (Boto3) to interact with S3 buckets.
* **Local Data Validation:** Ensures data quality and integrity before loading.
* **Data Transformation:** Processes and transforms data as needed for the target data mart or SQL database.
* **Data Loading:** Loads validated data into a data mart or SQL database.
* **Data Mart Storage:** Uses Parquet format with Snappy compression for efficient storage.
* **SQL Database Integration:** Supports loading data into SQL databases with a star schema (fact and dimension tables).
* **Logging:** Comprehensive logging to track project execution and potential issues.
* **Modular Design:** Well-structured codebase with modules for different functionalities.
* **Encryption:** Implements encryption for secure data handling.

## Technologies Used

* **Python:** Primary programming language.
* **Apache Spark:** Distributed data processing engine.
* **AWS S3:** Cloud storage for data.
* **AWS SDK for Python (Boto3):** Interaction with AWS services.
* **SQL:** Relational database interactions.
* **Parquet:** Columnar data storage format.
* **Snappy:** Data compression codec.
* **PyCharm:** Recommended IDE.
* **MySQL:** Database system (installation required).
```plaintext
Project structure:-
my_project/
├── docs/
│   └── readme.md
├── resources/
│   ├── __init__.py
│   ├── dev/
│   │    ├── config.py
│   │    └── requirement.txt
│   └── qa/
│   │    ├── config.py
│   │    └── requirement.txt
│   └── prod/
│   │    ├── config.py
│   │    └── requirement.txt
│   ├── sql_scripts/
│   │    └── table_scripts.sql
├── src/
│   ├── main/
│   │    ├── __init__.py
│   │    └── delete/
│   │    │      ├── aws_delete.py
│   │    │      ├── database_delete.py
│   │    │      └── local_file_delete.py
│   │    └── download/
│   │    │      └── aws_file_download.py
│   │    └── move/
│   │    │      └── move_files.py
│   │    └── read/
│   │    │      ├── aws_read.py
│   │    │      └── database_read.py
│   │    └── transformations/
│   │    │      └── jobs/
│   │    │      │     ├── customer_mart_sql_transform_write.py
│   │    │      │     ├── dimension_tables_join.py
│   │    │      │     ├── main.py
│   │    │      │     └──sales_mart_sql_transform_write.py
│   │    └── upload/
│   │    │      └── upload_to_s3.py
│   │    └── utility/
│   │    │      ├── encrypt_decrypt.py
│   │    │      ├── logging_config.py
│   │    │      ├── s3_client_object.py
│   │    │      ├── spark_session.py
│   │    │      └── my_sql_session.py
│   │    └── write/
│   │    │      ├── database_write.py
│   │    │      └── parquet_write.py
│   ├── test/
│   │    ├── scratch_pad.py.py
│   │    └── generate_csv_data.py
```

## Setup Instructions

1.  **Prerequisites:**
    * Install Apache Spark.
    * Install PyCharm.
    * Install MySQL.
    * Set up an AWS account and configure Boto3.
2.  **Project Setup:**
    * Clone the project repository.
    * Install the required Python dependencies (see `requirements.txt`).
    * Configure the project settings (e.g., AWS credentials, database connection details).
3.  **Running the Project:**
    * Follow the instructions in the project's documentation to execute the data processing pipeline.

## Project Structure

* `data_mart/`: Contains data mart files.
* `errors/`: Stores error logs and related files.
* `processing/`: Contains files related to data processing.
* `README.md`: Project documentation.
* `requirements.txt`: Python dependencies.

## Project Architecture

![Project Architecture Structure](https://github.com/sitanshu4933/DE_PROJECT_PRACTICE/blob/master/spark%20project/docs/architecture.png)

## Database ER Diagram

![Database ER Diagram Structure](https://github.com/sitanshu4933/DE_PROJECT_PRACTICE/blob/master/spark%20project/docs/database_schema.drawio.png)
