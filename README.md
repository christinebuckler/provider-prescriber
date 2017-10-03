# Provider Prescriber
This repo contains the documentation for my final capstone project at [Galvanize](https://www.galvanize.com/denver-platte/data-science#curriculum). The goal of this project is to effectively combine my programming and data science toolbox to demonstrate my analytical and critical thinking skills.

## Background
Healthcare providers are diverse and current information systems don’t always contain the most effective filters to find similar providers. Each healthcare provider is considered to have a unique NPI with exception for cases of fraud. There are many business use cases for this study, however, the ones I feel would be most pertinent include provider recommendations for:
* Patients that have changed insurance plans
* Pharmaceutical representatives selling specialty products

The public NPPES dataset (6GB) is available for download from [CMS](https://www.cms.gov/Regulations-and-Guidance/Administrative-Simplification/NationalProvIdentStand/DataDissemination.html). The current database has over 5 million entries and 328 available descriptor fields for each NPI. The BIGgest challenge in this study is working with BIG data. The method is straightforward but the computing resources must be optimized. Features used in this study include: entity type, gender, state of business location, specialties, credentials, sole proprietor status, and organizational subpart status.

## Objective
Provide the top 10 most similar providers given a specific National Provider Identifier (NPI). 
* Minimum Viable Product: compute similarity on subset of data
* Stretch Goal 1: Scale model to compute similarity on entire dataset
* Stretch Goal 2:  Account for monthly and weekly data updates

## Method
Brute force method compares each item to every other item which doubles the computation and memory storage with each addition to the input data set O(N^2) O(n<sup>2</sup>). The curse of dimensionality makes this a very challenging task. LSH reduces the dimensionality of high-dimensional data. 

To tackle this problem, I utilized MinHash LSH (Locality Sensitive Hashing) which is an efficient algorithm to find similar items using hashes. This technique approximates similarity within a threshold using the following steps:
1. Input binary vectors where non-zero values indicate presence of element/feature
2. Create hashes/slices of each item (increasing the number of hashes increases accuracy but also increases computational cost and run time)
3. Group items with similar hashes into buckets (option to set similarity threshold)
4. Calculate similarity distance between items in the same bucket

For this project, I used Apache Spark's implementation of MinHash LSH to take advantage of distributed computing to evaluate many parallel calculations. 






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


This is an unsupervised learning case study where a true target label does not exist. By joining this data with implicit or user ratings, the similarity matrix could be integrated into a recommender engine to predict the most similar NPIs based on user preferences.  
