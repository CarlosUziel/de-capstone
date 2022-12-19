<div id="top"></div>

<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#premise">Premise</a></li>
        <li><a href="#goal">Goal</a></li>
        <li><a href="#execution-plan">Execution Plan</a></li>
        <li><a href="#data">Data</a></li>
        <li><a href="#data-schema">Data Schema</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#setting-up-a-conda-environment">Setting up a conda environment</a></li>
        <li><a href="#setting-up-a-local-apache-airflow-server">Setting up a local Apache Airflow server</a></li>
        <li><a href="#getting-ready-to-interact-with-aws">Getting ready to interact with AWS</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#additional-notes">Additional Notes</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>

# Data Engineer Nanodegree Capstone Project

This is the final project from the [Data Engineer Nanodegree Program at Udacity](https://www.udacity.com/course/data-engineer-nanodegree--nd027) to practice everything that has been tought during the degree.

## About The Project

### Premise

Immigration affects the receiving country in many fronts, from the labor market to cultural changes. It is essential for policy makers to understand immigration trends to better address the needs of the ever-changing population, as well as to foster international cooperation with the countries immigrants come from.

<p align="right">(<a href="#top">back to top</a>)</p>

### Goal

The goal of this project is to extract insights from U.S. immigration data. Moreover, additional information can be obtained by correlating immigration data with U.S. cities demographics, available airports in the receiving cities and even average temperature through the year.

<p align="right">(<a href="#top">back to top</a>)</p>

### Execution plan

In order to achieve this goal, I will be using Amazon Web Services (AWS) S3 buckets, Apache Airflow and Apache Spark to populate a Data Lake residing on S3. Given the reasonable size of the data (available in this repository through [Git LFS](https://git-lfs.github.com/)), the ETL pipeline represented as an Airflow DAG will be run locally, as well as the subsequent analytic queries.

Raw data will be cleaned and uploaded to S3. Then, the STAR dimensional tables will be extracted and stored on S3 to form the Data Lake. Finally, these tables will be queried for different analytic purposes. This pipeline is meant to be run on-demand and not in a schedule, since the datasets are static and do not come from any streaming or updating source. All data operations are performed with Spark for best performance.

It is however worth considering whether this action plan would also be appropriate in different scenarios:

  1. **The data is 100x larger**:
      - The size of the data would make it infeasible to process on a standard consumer PC or laptop or even a medium-size compute server. Therefore it would make sense to use cloud computing services such as AWS EC2 machines or AWS EMR clusters for all data-heavy operations. Also, all data (i.e. from raw to cleaned data) would be stored in S3 or alternatively on HDFS partitions for quicker access from Spark.
  2. **The data populates a dashboard that must be updated on a daily basis by 7am every day**:
      - If the dashboard must be updated daily, that means the data is also increasing in size daily. Therefore, similar to the previous point, more computing resources would be needed to handle this additional requirement. The Airflow DAG would then need to be scheduled such that it would be done by 7am every day. This could be achieved by scheduling the pipeline to start a few hours earlier (depending on estimated total compute time) as well as by setting an [Service Level Agreement (SLA)](https://airflow.apache.org/docs/apache-airflow/1.10.10/concepts.html?highlight=slas#slas).
  3. **The database needed to be accessed by 100+ people**:
      - With the data stored in S3 buckets, this should not be an issue, as long as each user has the necessary permissions to access said S3 buckets. Amazon S3 buckets are designed to support [high frequency operations](https://aws.amazon.com/about-aws/whats-new/2018/07/amazon-s3-announces-increased-request-rate-performance/).

<p align="right">(<a href="#top">back to top</a>)</p>

### Data

The project uses the following data sets:

- **I94 Immigration Data**: This data comes from the US National Tourism and Trade Office and covers the year 2016. A data dictionary is available in `data/i94_inmigration_data_2016/schema.json`. This data set contains many records, a smaller data sample is available in `data/i94_inmigration_data_2016/data_sample.csv.bz2`.
- **World Temperature Data**: Global land and ocean-and-land temperatures ([source](https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data)).
- **U.S. City Demographic Data**: This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000 ([source](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/information/)).
- **Airport Code Table**: This is a simple table of airport codes and corresponding cities ([source](https://datahub.io/core/airport-codes#data)).

<p align="right">(<a href="#top">back to top</a>)</p>

### Data Schema

The Data Lake follows a STAR schema, where tables can be joined by a `city_id` field that uniquely identifies a city in a US state. A total of five tables were extracted: `dim_cities`, `dim_airports`, `fact_temps`, `fact_us_demogr` and `fact_immigration`. A brief description of each table, their source datasets as well as the available columns can be found in `data/star_schema.json`.

After running the whole project, the objects in S3 should be the same as in `data/s3_inventory.txt`. Data profiling reports for all tables except `facts_immigration` are available in `data/profiling_reports`.

<p align="right">(<a href="#top">back to top</a>)</p>

## Getting Started

To make use of this project, I recommend managing the required dependencies with Anaconda.

### Setting up a conda environment

Install miniconda:

```bash
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh
```

Install mamba:

```bash
conda install -n base -c conda-forge mamba
```

Install environment using provided file:

```bash
mamba env create -f environment.yml # alternatively use environment_core.yml if base system is not debian
mamba activate de_capstone
```

### Setting up a local Apache Airflow server

To start a local Apache Airflow server for the purposes of this project, simply run the following:

```bash
bash initialize_airflow.sh
```

Introduce your desired password when prompted and then access the UI at `localhost:8080` with user `admin` and the password you just created.

### Getting ready to interact with AWS

**Create an IAM user:**

  1. IAM service is a global service, meaning newly created IAM users are not restricted to a specific region by default.
  2. Go to [AWS IAM service](https://console.aws.amazon.com/iam/home#/users) and click on the "**Add user**" button to create a new IAM user in your AWS account.
  3. Choose a name of your choice.
  4. Select "**Programmatic access**" as the access type. Click Next.
  5. Choose the **Attach existing policies directly tab**, and select the "**AdministratorAccess**". This is solely for the purposes of this project and not recommended in a production environment. Click Next.
  6. Skip adding any tags. Click Next.
  7. Review and create the user. It will show you a pair of access key ID and secret.
  8. Take note of the pair of access key ID and secret. This pair is collectively known as Access key.

**Save access key and secret locally:**

  1. Create a new file, `_user.cfg`, and add the following:

      ```bash
      AWS_ACCESS_KEY_ID = <YOUR_AWS_KEY>
      AWS_SECRET_ACCESS_KEY = <YOUR_AWS_SECRET>
      ```

  2. This file will be loaded internally to connect to AWS and perform various operations.
  3. **DO NOT SHARE THIS FILE WITH ANYONE!** I recommend adding this file to .gitignore to avoid accidentally pushing it to a git repository: `printf "\n_user.cfg\n" >> .gitignore`.

**Set configuration values:**

Fill the `dl.cfg` configuration file. This is needed in order to create the S3 bucket that will hold all the data, as well as the region where this bucket should reside in.

<span style="color:red;font-weight:bold">*DO NOT FORGET TO DELETE YOUR S3 BUCKETS WHEN FINISHED WORKING ON THE PROJECT TO AVOID UNWANTED COSTS!*</span>

<p align="right">(<a href="#top">back to top</a>)</p>

## Usage

Simply follow along the main notebook of this project: `notebooks/main.ipynb`.

<p align="right">(<a href="#top">back to top</a>)</p>

## Additional Notes

Source files formatted using the following commands:

```bash
isort .
autoflake -r --in-place --remove-unused-variable --remove-all-unused-imports --ignore-init-module-imports .
black .
```

## License

Distributed under the MIT License. See `LICENSE` for more information.

<p align="right">(<a href="#top">back to top</a>)</p>

## Contact

[Carlos Uziel Pérez Malla](https://www.carlosuziel-pm.dev/)

[GitHub](https://github.com/CarlosUziel) - [Google Scholar](https://scholar.google.es/citations?user=tEz_OeIAAAAJ&hl=es&oi=ao) - [LinkedIn](https://at.linkedin.com/in/carlos-uziel-p%C3%A9rez-malla-323aa5124) - [Twitter](https://twitter.com/perez_malla)

<p align="right">(<a href="#top">back to top</a>)</p>

## Acknowledgments

This README includes a summary of the official project description provided to the students of the [Data Engineer Nanodegree Program at Udacity](https://www.udacity.com/course/data-engineer-nanodegree--nd027).

<p align="right">(<a href="#top">back to top</a>)</p>
