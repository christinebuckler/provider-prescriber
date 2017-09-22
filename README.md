# Provider Prescriber
This repo contains the documentation for my final capstone project at [Galvanize](https://www.galvanize.com/denver-platte/data-science#curriculum). The goal of this project is to effectively combine my programming and data science toolbox to predict the top 10 most similar NPIs based on a given provider NPI.

The data is a public dataset available for download from [www.cms.gov](https://www.cms.gov/Regulations-and-Guidance/Administrative-Simplification/NationalProvIdentStand/DataDissemination.html). The current file has over 5 million entries and 328 available fields for each NPI which makes my project's biggest challenge combatting computations on a very large dataset.  

This is an unsupervised learning case study where a true target label does not exist. By joining this data with implicit or user ratings, the similarity matrix could be integrated into a recommender engine to predict the most similar NPIs based on user preferences.  

### Use Cases:
* Patients look for similar providers to get second opinions on diagnosis
* Pharmaceutical reps look for similar providers to sell their products
* Patients moving to a new area look for similar providers based on their previous provider (if they liked them)

# Method  

### EDA & Data Mining:  
* Download public dataset from www.cms.org
* Load data subset locally
* Identify key features
* Decide how to label missing data
* Preprocess data into usable format
* Feature engineering
* Join with other useful datasets (taxonomy code data dictionary)

### Unsupervised Learning:
* Create Similarity Matrix and compare distance measures
* Create Graph and use to visual community and neighbors

### Stretch Goals
* Leverage big data tools, AWS and PySpark, to handle large dataset
* Accommodate weekly and monthly data updates
* Build web app platform for user interface
* Cluster data and visualize in 3D space

# Data Flow

(insert flow chart here)
(storage, input, processes, output, UI)
