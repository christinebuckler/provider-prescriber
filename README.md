# Provider Prescriber
This repo contains the documentation for my final capstone project at [Galvanize](https://www.galvanize.com/denver-platte/data-science#curriculum). The goal of this project is to effectively combine my programming and data science toolbox to demonstrate my analytical and critical thinking skills.

## Background
Healthcare providers are diverse and current information systems don’t always contain the most effective filters to find similar providers. Each healthcare provider is considered to have a unique NPI with exception for cases of fraud. What is an NPI? An NPI (National Provider Identifier) is a 10-digit intelligence-free numeric identifier that is given to each healthcare provider and organization. 

![NPI_num](/images/NPI_num.png)

There are many possible business use cases for this study, however, the ones I feel would be most pertinent include provider recommendations for:
* Pharmaceutical representatives selling specialty products to similar providers
* Patients that have changed insurance plans and are looking for a similar provider

![providers](/images/providers.png) ![NPPES](/images/NPPES.png)

The public dataset (6GB) is available for download from [CMS](https://www.cms.gov/Regulations-and-Guidance/Administrative-Simplification/NationalProvIdentStand/DataDissemination.html). The current database has over 5 million entries and 328 available descriptor fields for each NPI. The BIGgest challenge in this study is working with BIG data. The method is straightforward but the computing resources must be optimized. Features used in this study include: entity type, gender, state of business location, specialties, credentials, sole proprietor status, and organizational subpart status.

## Objective
Provide the top 10 most similar providers given a specific National Provider Identifier (NPI). 
* Minimum Viable Product: compute similarity on subset of data
* Stretch Goal 1: Scale model to compute similarity on entire dataset
* Stretch Goal 2:  Account for monthly and weekly data updates

## EDA & Data Mining:  
* Download public dataset from www.cms.org
* Load data subset locally
* Identify key features
* Decide how to label missing data
* Preprocess data into usable format
* Feature engineering
* Join with other useful datasets (taxonomy code data dictionary)

## Method
Brute force method compares each item to every other item which doubles the computation and memory storage with each addition to the input data set O(n<sup>2</sup>). The curse of dimensionality makes this a very challenging task. LSH reduces the dimensionality of high-dimensional data. 

To tackle this problem, I utilized MinHash LSH (Locality Sensitive Hashing) which is an efficient algorithm to find similar items using hashes. This technique approximates similarity within a threshold using the following steps:
1. Input binary vectors where non-zero values indicate presence of element/feature
2. Create hashes/slices of each item (increasing the number of hashes increases accuracy but also increases computational cost and run time)
3. Group items with similar hashes into buckets (option to set similarity threshold)
4. Calculate similarity distance between items in the same bucket

![MinHashLSH](/images/MinHashLSH.png)

Below is an overview of the process steps which begins with data exploration and data munging. The model requires that the input be transformed into binary vectors which was done by parsing the csv in Python. I used Apache Spark's implementation of MinHash LSH to take advantage of distributed computing to evaluate many parallel similarity calculations. The PySpark script was executed on an AWS virtual machine for additional computing power and resources. The output csv was uploaded to a Postgres database where it is available to be queried by users. 

![ProcessFlow](/images/ProcessFlow.png)

## Measures
The Jaccard distance measure considers the relationship between intersection and union. There are several variations of Jaccard that solve for similarity versus disimilarity. Below is the equation used in this study where distances close to zero indicate high similarity; distances close to one indicate high dissimilarity.

$$
\d(A,B) = 1 - \frac{\left |A\cap B  \right |}{\left |A\cup B  \right |}
$$

![JaccardSimilarity](/images/JaccardSimilarity.png)

Below is a simple example of how similarity is calculated between items.

![SimilarityExample](/images/SimilarityExample.png)

False Positives occur when a pair of dissimilar items are grouped in the same bucket and add noise to the system. False Negatives occur when a pair of similar items are *not* grouped in the same bucket and will never be compared. False Negatives are more detrimental for analysis; consider this equivalent to never finding your soul mate!

![FPFN](/images/FPFN.png)

This is an unsupervised learning case study where a true target label does not exist. With a labeled target, accuracy, precision and recall could be calculated to evaluate the predictive power of the model. 

## Results
I was able to compute similarity distances for a subset of the data (10,000 NPIs) and store inside a database which can be queried for specific NPIs. Try it out for yourself [here](https://buckler-pcd.firebaseapp.com/). (website created by Galvanize Web Dev student Chris White). Note: Only NPIs in this subset are currently included in the database. Need help finding your provider's NPI? Search [here](https://npiregistry.cms.hhs.gov/registry/).

## Next Steps
With more time, I would like to explore the following areas:
* Improve virtual machine configuration to scale for more items
* Expand input method to allow for updates without re-hashing existing data
* Evaluate other features that add value to similarity measure such as standardized provider ratings
* Integrate query with NPPES API to give context to the results
* Add functionality to search for similar providers based on a list of NPIs
* Cluster or graph items to visualize groupings

## References
Pyspark documentation
online publications

## Credits
Special thanks to the Galvanize instructors, DSRs, mentors, classmates and my family for their support and encouragement during my immersive experience! 
