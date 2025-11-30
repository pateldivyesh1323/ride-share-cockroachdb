# Geo-Distributed Ride-Sharing Database System

## Setup Python Project

Create a virtual environment:

```bash
python -m venv venv
```

Activate the virtual environment:

On Windows:

```bash
venv\Scripts\activate
```

On Linux/Mac:

```bash
source venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

## Run FastAPI Server

Start the FastAPI development server:

```bash
fastapi dev server/main.py
```

The API will be available at `http://localhost:8000`

## Initialize the CockroachDB Clusters

Start docker container:

```bash
docker-compose up -d
```

Initialize the cluster (Run this once):

```bash
docker exec -it roach-east-1 ./cockroach init --insecure
```

Open `localhost:8080` in browser to visualize the CockroachDB dashboard.

## Create Database and Assign Regions

You need an enterprise license to enable multi-region features in CockroachDB.

Set the license key in roach shell:

```sql
SET CLUSTER SETTING enterprise.license = 'YOUR-CRDB-KEY-HERE';
```

Create Database:

```sql
CREATE DATABASE rideshare;
```

Assign a primary region:

```sql
ALTER DATABASE rideshare PRIMARY REGION "us-east";
```

Assign secondary regions:

```sql
ALTER DATABASE rideshare ADD REGION "us-west";
ALTER DATABASE rideshare ADD REGION "eu-central";
ALTER DATABASE rideshare ADD REGION "ap-south";
```

## Generate Data

Generate sample data:

```bash
python data_generation.py
```

## Load Data

Load sample data:

```bash
python load_generated_data.py
```

To delete data and load again:

```bash
python load_generated_data.py --clear
```

To just delete data from database:

```bash
python load_generated_data.py --delete-only
```
