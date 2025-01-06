import pandas as pd
import numpy as np
import logging 

# setup the logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def  extract_data(file_path):

    # extract data from csv file
    try:
        df = pd.read_csv(file_path)
        logger.info('Data extracted successfully.')
        return df
    except Exception as e:
        logger.error(f'Error extracting data {e}')
        raise




def transform_data(data):
    df =  data.copy()
    # transforming the data 
    try :
        # Correctly replace period with underscore
        df['job'] = df['job'].str.replace(r'\.', '_', regex=True)

        # Replace '.' with '_'
        df['education'] = df['education'].str.replace(r'\.', '_', regex=True)

        # Replace 'unknown' with NaN
        df['education'] = df['education'].replace('unknown', np.NaN)
        # Clean and convert client columns to bool data type
        for col in ["credit_default", "mortgage"]:
            df[col] = df[col].apply(lambda x: 1 if x == "yes" else 0)
            df[col] = df[col].astype(bool)

        # Convert previous_outcome to binary values
        df["previous_outcome"] = df["previous_outcome"].map({"success": 1,"failure": 0,"nonexistent": 0})

        df["campaign_outcome"] = df["campaign_outcome"].map({"yes": 1,"no": 0})
        
        # Add year column
        df["year"] = "2022"

        # Convert day to string
        df["day"] = df["day"].astype(str)

        # Add last_contact_date column
        df["last_contact_date"] = df["year"] + "-" + df["month"] + "-" + df["day"]

        # Convert to datetime
        df["last_contact_date"] = pd.to_datetime(df["last_contact_date"], 
                                                    format="%Y-%b-%d")

        # Clean and convert outcome columns to bool
        for col in ["campaign_outcome", "previous_outcome"]:
            df[col] = df[col].astype(bool)

        # Drop unneccessary columns
        df.drop(columns=["month", "day", "year"], inplace=True)
        logger.info('Data Transforemed successfully.')
        return df
    except Exception as e:
        logger.error(f'Error Transforming data {e}')
        raise


def load_data(df, output_path):
    try:
        df.to_csv(output_path, index=False)
        logger.info('Data Loading successfully.')
    except Exception as e:
        logger.error(f'Error loading data {e}')
        raise

def etl():
    try:
        input_file_path = "bank_marketing.csv"
        output_file_path = "Transformed_bank_marketing.csv"

        marketing_df =  extract_data(input_file_path)
        cleaned_df = transform_data(marketing_df)
        load_data(cleaned_df,output_file_path)
        logger.info("ETL process completed successfully")
    except Exception as e:
        logger.error(f'Error ETL process failed  {e}')
    

if __name__ =='__main__':
    etl()



# Step 5: Save the DataFrames


print("Files saved successfully!")



# print("mareketing data  data",marketing_df.head())
# print("cleaned data",cleaned_df.head())




# print('markering df',marketing_df.loc[marketing_df['job'].str.contains('unknown', na=False), 'job'])
# print('cleaned df',cleaned_df.loc[cleaned_df['job'].str.contains('unknown', na=False), 'job'])

# print(marketing_df['mortgage'].unique())
# print(cleaned_df['mortgage'].unique())
# Map 'yes' to 1, and all other values ('no' and 'unknown') to 0


# data : client_id,age,job,marital,education,credit_default,mortgage,month,day,contact_duration,number_contacts,previous_campaign_contacts,previous_outcome,cons_price_idx,euribor_three_months,campaign_outcome
