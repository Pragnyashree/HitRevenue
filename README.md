HitRevenue

HitRevenue is a Cloud-Native Flask webapp that accepts one tab separated data file(tsv) in the format provided in the requirement document and returns the processed output
file with name like '[Date]_SearchKeywordPerformance.tab'.

HitRevenue is configured to be deployed in AWS Elastic Beanstalk. The deployment steps are provided later in this document.

The final output of this app is a tab delimited file with the following headers with data sorted by revenue in descending order:
•Search Engine Domain
•Search Keyword 
•Revenue 

Application is hosted on AWS you can check it here:
AWS URL: http://hitrevenue-env-1.eba-p8zun3r4.us-east-1.elasticbeanstalk.com/

Folder Structure:

HitRevenue/
├─ modules/
│  ├─ DataTransformation.py
├─ templates/
│  ├─ 500.html
│  ├─ 404.html
│  ├─ index.html
├─ tests/
│  ├─ unit/
│  │  ├─ test_class_functions.py
├─ upload/
├─ application.py
├─ requirements.txt
├─ README.md



Execution Steps:
You need to have an AWS account set up before deployment. You can get a free tier account set up at their website.
1) Git clone this project to your local machine.
2) Zip all the folder contents into a single folder.
3) Login to your AWS console and open Elastic Beanstalk.
4) Create a Elastic Beanstalk application and add existing project which is the zip folder from local.
5) EBS will build and deploy automatically and maintain versions for each new deployment.

Assumptions:
1) For the product_list column, data length of each row is homogeneous for a given file.
2) The product_list column data is as described in the requirement document.
3) All the datatypes mentioned for the hit-level data are same for all the new files.
4) Purchases were made on the same date when the search engine URL was checked and not the next day.


Limitations:

1) May fail for very huge file size.

Improvement:

1) Use pyspark instead of pandas for parallel processing of data in a distributed manner and better scalability.
2) Data encryption while file upload and download.





