import pandas as pd
import numpy as np
import pandasql as ps
import os
import re


class DataTransformation:

    # This function reads the input hit level data file sent by client and returns the modified dataframe
    @staticmethod
    def read_file(file):
        # Read the tsv file into a dataframe
        hit_data_df = pd.read_csv(file, sep='\t')
        # Convert the date_time column's datatype from object to datetime
        hit_data_df['date_time'] = pd.to_datetime(hit_data_df['date_time'])
        # Create another column containing only the date values from date_time column
        hit_data_df['date'] = hit_data_df['date_time'].dt.date

        return hit_data_df

    """ This function converts each row of data in product_list to lists.
     It creates a list of NaN(based on total number of columns) for all NaN values in product_list
     Converts the semicolon separated text data to list for usage at a later stage
     returns error in case any other datatype apart from float(NaN) or string is encountered """

    @staticmethod
    def transform_column(column_value):
        # Condition to check for string object, if true split the string with ';'or '|' as delimiter
        if isinstance(column_value, str):
            row = re.split(r';|\|', column_value)
            row = pd.Series(row, dtype=object).fillna(0).tolist()
            return row
        # When column_value is not str, return NaN
        else:
            row = [np.nan]
            return row

    """ This function takes words as input and performs manipulations and returns the word in as String
        If the incoming word is not a string instance the function returns NaN """

    @staticmethod
    def extract_search_keyword(word):
        # Condition to check if the word is a string object. If not return NaN
        if isinstance(word, str):
            word = str(word).strip()
            word = word.replace("&", '')  # Replace the character '&' with ''
            word = word.replace('+', ' ')  # Replace the character '+' with ' '
            word = word.split('=')[1]  # Split the word with delimited '=' and return the 2nd word in the list
            return word
        else:
            return np.nan

    """This method is used to parse the text data in product_list column and convert it to a dataframe
    with 5 columns at the moment. Each row in product_list is a semicolon separated text or NaN.
    Also two new columns('ip','date') are added as primary and foreign key to the main hit_data_dataframe"""

    def parse_product_list(self, hit_data_df):
        # Run the lambda function on transform_column by passing each row in product_list column in hit_data_df
        # rows contains a list of words which were originally semicolon or pipe delimited text in product_file column
        rows = hit_data_df['product_list'].apply(lambda x: self.transform_column(x)).to_list()
        # Create product_list_df with the mentioned columns and pass rows as data
        product_list_df = pd.DataFrame(data=rows)
        product_list_df = product_list_df.rename(
            columns={0: "Category", 1: "Product_Name", 2: "Number_of_Items", 3: "Total_Revenue", 4: "Custom_Event",
                     5: "Custom_event2", 6: "Merchandizing eVar"})
        # Replace empty values with 0
        product_list_df = product_list_df.replace('', 0)
        product_list_df = product_list_df.fillna(np.nan)
        # Copy 'ip' and 'date' columns from  hit_data_df to product_list_df
        product_list_df['ip'] = hit_data_df['ip']
        product_list_df['date'] = hit_data_df['date']
        return product_list_df

    """This method extracts the domain name from the search URL in 'referrer column and adds it to 
    a new column called 'Search_Domain' in hit_data_df"""

    @staticmethod
    def extract_domain(hit_data_df):
        # Creates a column 'Search_Domain' by executing a regex on the 'referrer' column of hit_data_df
        hit_data_df['Search_Domain'] = hit_data_df['referrer'].str.extract(r'([a-zA-Z0-9]*.[a-zA-Z0-9]*.com)')
        return hit_data_df

    """This method extracts the keyword from the search URL in 'referrer' column and adds it to 
    a new column called 'Search_Keyword' in hit_data_df"""

    def extract_keyword(self, hit_data_df):
        # Creates a column 'Search_Keyword' by executing a regex on the 'referrer' column of hit_data_df
        hit_data_df['Search_Keyword'] = hit_data_df['referrer'].str.extract(r'([&,?][q,p]=[A-Za-z0-9+]*&)')
        # Call the extract_search_keyword method to refine the keyword
        hit_data_df['Search_Keyword'] = hit_data_df['Search_Keyword'] \
            .apply(lambda x: self.extract_search_keyword(x))
        return hit_data_df

    """ This method performs all the data transformations and 
    accepts filepath and app root directory path as arguments
    and returns output file name"""

    def generate_output(self, file, root_path):
        try:
            # Read the input file
            hit_data_df = self.read_file(file)
            # Get the product_list_df using the parse_product_list method   
            product_list_df = self.parse_product_list(hit_data_df)
            hit_data_df = self.extract_domain(hit_data_df)
            hit_data_df = self.extract_keyword(hit_data_df)

            """ Run sql query to get rows that have the domain name not as domain where event_list is 1.0
            and ip is same as event_list is 1.0"""
            processed_df = ps.sqldf("SELECT * FROM hit_data_df "
                                    "WHERE Search_Domain NOT IN "
                                    "(SELECT Search_Domain from hit_data_df WHERE event_list = 1.0)"
                                    "AND "
                                    "ip IN (SELECT ip "
                                    "FROM hit_data_df "
                                    "WHERE event_list = 1.0) ORDER BY ip ")
            # Run sql query to get total revenue for certain ip on certain date
            revenue_df = ps.sqldf("SELECT ip,date,SUM(Total_Revenue) AS Revenue "
                                  "FROM product_list_df "
                                  "GROUP BY ip,date")
            # Perform merge on ip between processed_df and revenue_df to eliminate ips that do not have revenue info
            processed_df = processed_df.merge(revenue_df, on='ip', how='left')
            # Extract the 3 required output columns to output_df
            output_df = processed_df[['Search_Domain', 'Search_Keyword', 'Revenue']]
            # Sort by descending order of revenue in output_df
            output_df = ps.sqldf("select * from output_df ORDER BY Revenue DESC")
            execution_date = str(pd.to_datetime('today').date())
            # Renaming columns to requisite format
            output_df = output_df.rename(columns={"Search_Domain": "Search Engine Domain",
                                                  "Search_Keyword": "Search Keyword"})
            # Save output file to mentioned path
            output_df.to_csv(os.path.join(root_path, 'upload/'
                                          + execution_date + '_SearchKeywordPerformance.tab'), sep="\t", index=False)
            return execution_date + '_SearchKeywordPerformance.tab'
        except Exception as e:
            print(e)
