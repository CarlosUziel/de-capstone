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
        <li><a href="#data">Data</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#setting-up-a-conda-environment">Setting up a conda environment</a></li>
        <li><a href="#setting-up-a-local-apache-airflow-server">Setting up a local Apache Airflow server</a></li>
        <li><a href="#setting-up-an-amazon-redshift-cluster">Setting up an Amazon Redshift cluster</a></li>
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

The goal of this project is to extract insights from U.S. immigration data. Additional information can be drawn by correlating immigration data with U.S. cities demographics, airports and even average temperature.

In order to achieve this goal, I will be using AWS services such as S3 buckets and Amazon Redshift. The ETL pipeline will be executed through an Apache Airflow DAG.

Raw data will first be uploaded to S3 buckets. Then, an Apache Airflow DAG will clean and process the data before creating and populating the relevant fact and dimension tables, following a predefined STAR schema. Finally, these end tables will be queried to obtain the desired insights.

<p align="right">(<a href="#top">back to top</a>)</p>

### Data

The project uses the following data sets:

- **I94 Immigration Data**: This data comes from the US National Tourism and Trade Office and covers the year 2016. A data dictionary is available in `data/i94_inmigration_data_2016/schema.json`. This data set contains many records, a smaller data sample is available in `data/i94_inmigration_data_2016/data_sample.csv.bz2`.
- **World Temperature Data**: Global land and ocean-and-land temperatures ([source](https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data)).
- **U.S. City Demographic Data**: This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000 ([source](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/information/)).
- **Airport Code Table**: This is a simple table of airport codes and corresponding cities ([source](https://datahub.io/core/airport-codes#data)).

<p align="right">(<a href="#top">back to top</a>)</p>

### Data Schema

To be defined...

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

### Setting up an Amazon Redshift cluster

**Create an IAM user:**

  1. IAM service is a global service, meaning newly created IAM users are not restricted to a specific region by default.
  2. Go to [AWS IAM service](https://console.aws.amazon.com/iam/home#/users) and click on the "**Add user**" button to create a new IAM user in your AWS account.
  3. Choose a name of your choice.
  4. Select "**Programmatic access**" as the access type. Click Next.
  5. Choose the **Attach existing policies directly tab**, and select the "**AdministratorAccess**". Click Next.
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

**Create cluster:**

  1. Fill the `dl.cfg` configuration file. These are the basic parameters that will be used to operate on AWS. More concretely, `GENERAL` covers general parameters, `DWH` includes the necessary information to create and connect to the Redshift cluster and S3 contains information on where to find the source dataset for this project. *This file is already filled with example values*.
  2. To create the Redshift cluster, simply run the `setup.py` python script (must be done after `initialize_airflow.sh`, since registration of connections is also taking place in `setup.py`).

<span style="color:red;font-weight:bold">*DO NOT FORGET TO TERMINATE YOUR REDSHIFT CLUSTER WHEN FINISHED WORKING ON THE PROJECT TO AVOID UNWANTED COSTS!*</span>

<p align="right">(<a href="#top">back to top</a>)</p>

## Usage

...

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
