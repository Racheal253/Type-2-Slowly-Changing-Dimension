<img width="975" height="503" alt="image" src="https://github.com/user-attachments/assets/7b29312c-701d-401d-83af-f29ee19d1d21" />


# Type-2-Slowly-Changing-Dimension

### 1.  INTRODUCTION 

In real-world HR data, employees change departments, job titles, or locations over time. Tracking these 
changes accurately is crucial for HR analytics, reporting, and regulatory compliance. However, many 
organizations still overwrite old data, losing valuable historical insights. 
In this project, we will implement Slowly Changing Dimension Type 2 (SCD Type 2) using PySpark within 
Microsoft Fabric Lakehouse to maintain historical accuracy and current state simultaneously, using a clean, 
scalable pipeline.

#### What is SCD 
Slowly Changing Dimensions (SCD) is a technique in data warehousing to manage and track changes in 
dimension tables (like employees, products, customers) over time. 
#### Types of SCD: 
- SCD Type 1: Overwrites old data with new data. No history is kept.
- SCD Type 2: Adds a new row for every change, preserving history. 
-  SCD Type 3: Tracks limited historical data using additional columns.

#### What is SCD Type 2? 
SCD Type 2 tracks full history of changes in dimension data by adding a new row for every change in an attribute 
(e.g., department, job title) while marking old records as inactive but maintaining them in the table. It uses 
StartDate to indicate when the record became active, EndDate to indicate when the record stopped being active 
(or blank if still active), and IsCurrent to indicate 'Y' if currently active and 'N' otherwise. 

Example:
<img width="975" height="185" alt="image" src="https://github.com/user-attachments/assets/a8b498f8-dc70-47ca-bdb7-ef4d4e07e612" />

This allows analysis like:
- Who was in the IT department in Jan 2024?
- 	When did Allison Hill move to HR?

### 2.	BUSINESS SCENARIO
The organization maintains an employee_master table containing employee details such as name, department, and job title. As employees transition between departments or change job titles over time, it is essential to capture and preserve the history of these changes for accurate reporting and analysis. To support this, SCD Type 2 logic is implemented in the dim_employee dimension table to track all historical changes while retaining previous records for reference.

### 3.	DATA SOURCE AND FORMAT:
- Source: GitHub
- Format: CSV 

### 4.	IMPLEMENTATION STEPS

#### A.	Data Ingestion
The Data was Ingested directly from Github using fabric pipeline copy activity and then loaded to the Lakehouse.

<img width="968" height="528" alt="image" src="https://github.com/user-attachments/assets/90bcf234-93a9-4fbe-8631-0736f3bc3ed2" />

#### B.	Loaded data with Pyspark using Fabric’s Pyspark Notebook and also importing necessary packages. 
from pyspark.sql import *
from pyspark.sql.functions import* 
from pyspark.sql.types import *
from pyspark.sql.window import *

df1 = spark.read.csv("Files/Employee",header=True,inferSchema=True)

display(df1)

df1.printSchema()


<img width="816" height="384" alt="image" src="https://github.com/user-attachments/assets/309fc9d2-eb21-4ada-a185-d1ba4410f48f" />

#### C.	Removing nulls or NaT from the loadDate and replacing them with current date

<img width="916" height="496" alt="image" src="https://github.com/user-attachments/assets/164ea033-d362-4b89-841a-fad6043a4604" />

#### D.	Converting LoadDate to Date Data type

<img width="866" height="539" alt="image" src="https://github.com/user-attachments/assets/9d474d90-ea16-4c18-bfab-6318c72dec8a" />

#### E.	Dropping Duplicates

<img width="883" height="595" alt="image" src="https://github.com/user-attachments/assets/ac256587-1d03-4117-a4fd-42fafe4cf6e5" />

#### F.	Using PySpark window functions to partition by EmpID, order by LoadDate, and identify changes by comparing each row’s values with those of the previous row and use lead() to get the next load date for each employee

<img width="1000" height="525" alt="image" src="https://github.com/user-attachments/assets/e66e55bd-5f99-4934-bf53-27624ee29bfd" />


#### G.	Compute StartDate, EndDate, IsCurrent

<img width="904" height="572" alt="image" src="https://github.com/user-attachments/assets/6b5a910b-3de6-4b56-bce8-e2ed331e2bcb" />

#### H.	Select only required columns

<img width="888" height="583" alt="image" src="https://github.com/user-attachments/assets/9cafcbfb-84a5-4d8c-b96b-748567d87799" />

#### I.	Save to Delta Table

<img width="906" height="581" alt="image" src="https://github.com/user-attachments/assets/574afeaa-5ca8-414f-aa44-966177e00525" />

### 5.	CHALLENGES FACED
- Handling missing and inconsistent dates: Dealt with NaT and null LoadDate issues while ensuring correct chronological ordering for SCD Type 2 tracking using consistent date handling and current_date() for nulls.
-	Understanding and applying PySpark window functions: Initially found partitionBy, orderBy, and lag challenging but overcame this by building small tests to visualize how window logic enables change detection efficiently.
-	Managing dynamic EndDate logic: Ensured EndDate updates correctly to “next LoadDate - 1” while leaving it blank for current records, avoiding off-by-one errors and duplicate active records IN THE PIPELINE.
### 6.	CONCLUSION
Implementing Slowly Changing Dimension (SCD) Type 2 in Microsoft Fabric Lakehouse using PySpark has provided a practical, scalable solution for tracking historical changes in employee records without losing critical historical context.



