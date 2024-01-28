# Import libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError 
import os
from datetime import datetime as dt
import yaml 
import re
from itertools import chain

# Function to connect to S3

def connect_to_s3():
    # Status
    print("Connecting to S3...")

    # Read in YAML for AWS creds
    # Specify the path to your YAML file
    yaml_fp = '/Users/samivanecky/git/fut/secrets/aws_s3.yaml'

    # Read the YAML file
    with open(yaml_fp, 'r') as file:
        creds = yaml.safe_load(file)

    # Set working directory for data
    os.chdir('/Users/samivanecky/git/fut/data/')

    # Define S3 resource
    s3 = boto3.resource(
        service_name='s3',
        region_name=creds['region_name'],
        aws_access_key_id=creds['aws_access_key_id'],
        aws_secret_access_key=creds['aws_secret_access_key']
    )

    # Return S3 object
    return(s3)

# Function to get list of players
# This should only need to be run occasionally to store links in db
# Links should be static but need to account for new players being added
def get_plyr_links():
    # Get base webpage URL
    base_url = 'https://www.futwiz.com/en/fc24/players?page='

    # Define list to hold all links
    all_lnks = []

    # Iterate over possible pages and get links
    for i in range(1200):
        print(f"Getting links for page {i}")

        # Set URL
        url = base_url + str(i)

        # Get webpage html
        try:
            pg_html = requests.get(url)
        except:
            print(f"There was an error getting data for {url}")
        
        # Extract webpage content
        pg_soup = BeautifulSoup(pg_html.content, "html.parser")

        # Get all the links on the page
        pg_lnks = pg_soup.find_all('a', href=True)

        # List to hold links
        lnks = []

        # Extract link components
        for l in pg_lnks:
            value = l['href']
            if '/en/fc24/player/' in value and 'page=' not in value:
                lnks.append(value)

        # Remove any dups
        lnks = list(set(lnks))

        # Check to see if the page is still returning valid results
        if len(lnks) > 1:
            # Add to the list of all links
            all_lnks.append(lnks)
        else:
            print("No more results to gather. Terminating loop...")
            break

    # Return final data
    return(all_lnks)

# Function to get player page data
def get_plyr_pricing(url):

    # Set URL
    base = "https://www.futwiz.com" + str(url)

    # Get player ID
    plyr_id = base.rsplit('/', 1)[-1]

    # Get the basic HTML from the page
    try:
        html = requests.get(base)
    except:
        print(f"error getting data for {base}")
        return(None)

    # Get content into BS4
    plyr_soup = BeautifulSoup(html.content, "html.parser")

    # Get player name
    plyr_name = plyr_soup.find(class_ = "playername").text

    # Get player price
    plyr_price = plyr_soup.find(class_ = "price-num").text

    # Get current timestamp
    current_timestamp = dt.now().strftime("%Y-%m-%d_%H-%M-%S")

    # Create dictionary for player
    plyr = {
        'name': plyr_name,
        'id': plyr_id,
        'price': plyr_price,
        'ts': current_timestamp
    }

    # Convert to data frame
    plyr_df = pd.DataFrame(plyr)

    # Return data
    return(plyr_df)

# Function to get player details (one-time run)
# This should only need to be used on initial load since player details beyond price don't change
def get_plyr_info(url):

    # Set URL
    base = "https://www.futwiz.com" + str(url)

    # Get player ID
    plyr_id = base.rsplit('/', 1)[-1]

    # Get the basic HTML from the page
    try:
        html = requests.get(base)
    except:
        print(f"error getting data for {base}")
        return(None)

    # Get content into BS4
    plyr_soup = BeautifulSoup(html.content, "html.parser")

    # Get player name
    plyr_name = plyr_soup.find(class_ = "playername").text

    # Get player stats
    plyr_stats = plyr_soup.find(class_ = "player-stats-grid").text
    # Remove new lines and white space
    # plyr_stats = re.sub(r"\n", " ", plyr_stats)
    # plyr_stats = re.sub(r"\s+", " ", plyr_stats)

    # Get player details
    plyr_dets = plyr_soup.find(class_ = "player-details-inner").text
    # Remove new lines and white space
    # plyr_stats = re.sub(r"\n", " ", plyr_stats)
    # plyr_stats = re.sub(r"\s+", " ", plyr_stats)

    # Get player playstyles
    plyr_playstyles = plyr_soup.find_all(class_ = "player-playstyle-info")
    
    # Iterate over playstyles and convert them to a text array
    # Removing any duplicated values due to webpage formatting
    styles = list(set([s.text for s in plyr_playstyles]))

    # Get current timestamp
    current_timestamp = dt.now().strftime("%Y-%m-%d_%H-%M-%S")

    # Create dictionary for player
    plyr = {
        'name': plyr_name,
        'id': plyr_id,
        'stats': plyr_stats,
        'details': plyr_dets,
        'playstyles': [styles],
        'ts': current_timestamp
    }

    # Convert to data frame
    plyr_df = pd.DataFrame(plyr)

    # Return data
    return(plyr_df)

# Function to write to S3 bucket
# Read in filepath for CSV
def upload_to_s3(s3, fp):
    # Print status
    print("Uploading to S3...")

    # Define s3 bucket name
    bucket_name = 'sgi-fut'
    try:
        # Upload the CSV file to S3
        s3.Bucket(bucket_name).upload_file(Filename=fp, Key=fp)
        print("Upload to S3 was successful.")
    except:
        print(f"Error uploading {fp} to S3.")

# Define main function
def main():

    # Connect to S3
    s3 = connect_to_s3()

    # Get links of players
    all_lnks = get_plyr_links()

    # Unest links
    all_lnks = list(chain(*all_lnks))

    # Iterate over links to get the player details
    for l in all_lnks:
        print(f"Getting data for: {l}")

        try:
            # Get player data
            plyr_df = get_plyr_info(l)
        except:
            print(f"Issue getting data for {l}")

        # Try and append to the overall dataframe
        try:
            all_plyrs_df = pd.concat([all_plyrs_df, plyr_df])
        except:
            # Instance where dataframe has not been created yet
            print(f"Setting data frame to be {l}")
            all_plyrs_df = plyr_df

    # Get NFL betting lines from DK and write to CSV
    # Returns file path for writing to S3
    # fp = getNflLines(url)

    # Upload to S3
    upload_to_s3(s3, fp)

# Basic run block
if __name__ == "__main__":
    main()

