import requests
import pandas as pd
from pandas import json_normalize  # We need to perform json_normalize to make nice dataframe out of extracted data
import math

# Read the CSV with collection addresses and collection_slug
csv_path = (r"your\csv\path.csv")
df_addresses = pd.read_csv(csv_path)
df_addresses.reset_index(drop=True, inplace=True)

np_addresses = df_addresses.to_numpy()  # Making an array to save dataframe it will be faster to loop through later

# Initial URL and headers (Info: there will be two requests in code 1st request to get total token count and later request will be to extract all tokens in a collection)
base_url = "https://data-api.nftgo.io/eth/v2/collection/{}/filtered_nfts"
headers = {
    "accept": "application/json",
    "X-API-KEY": "your_API_key"
} ## IMPORTANT: This is my API key I kept it here if you just wanted to quickly try code for 1 or 2 collection addresses in CSV. Do not share

# Making a dictionary to save the final DataFrames containing all tokens from a collection
dfs_dict = {}

# 1st loop to extract collection_address and collection_slug from csv/array based on rows
for item in range(len(np_addresses)):
    collection_address = np_addresses[item][0]  # Assuming Collection_Address is the first column
    collection_slug = np_addresses[item][1]     # Assuming collectionSlug is the second column
    print(f"Collection Address: {collection_address}, Collection Slug: {collection_slug}")


    # Placeholder for the final dataframe containing all the token for the current collection address
    df_final = pd.DataFrame()

    # Initial cursor; cursor is important to get to next page containing tokens. Each page has max 50 tokens we can extract from 1 request
    cursor = 'cursor'

    # Making the initial request to get total count; the tokens in collection are sorted according to the rarity rank in descending order
    initial_url = f"{base_url.format(collection_address)}?sort_by=rarity_high_to_low&is_listing=false&cursor={cursor}&limit=1"
    initial_response = requests.get(initial_url, headers=headers)

    # Checking if the initial request was successful
    if initial_response.status_code == 200:
        # Extracting total token count from the initial response that are saved in temporary dictionary = 'initial_data'
        initial_data = initial_response.json()
        total_count = initial_data.get("total", 0)

        # Calculating the number of iterations needed
        num_iterations = math.ceil(total_count / 50)

        # Fetching tokens for collection address/collection_slug according to their rarity ranking in descending order
        print(f"Fetching data for collection_slug '{collection_slug}' ({total_count} tokens)")

        # 2nd loop: Loop for fetching data in chunks (from one page)
        for i in range(num_iterations):
            # Constructing the URL with the current cursor (to go to subsequent page)
            current_url = f"{base_url.format(collection_address)}?sort_by=rarity_high_to_low&is_listing=false&cursor={cursor}&limit=50"

            # Making the API request
            response = requests.get(current_url, headers=headers)

            # Check if the request was successful
            if response.status_code == 200:
                # Extract data from the response and saving to dictionary = 'data'
                data = response.json()

                # Extracting the next cursor for the next iteration
                cursor = data.get("next_cursor")

                # Normalizing the data and appending it to the final dataframe. Important to make better dataframe rather than messy: df is temporary dataframe containing tokens from only one page
                df = json_normalize(data["nfts"])

                # Since we are getting 20+ columns, keeping only useful ones
                columns_to_keep = ['blockchain', 'name', 'contract_address', 'token_id', 'collection_slug']
                df = df[columns_to_keep]

                # Concatenating data points from temporary df to permanent df (for single collection)
                df_final = pd.concat([df_final, df], ignore_index=True)

                # While code is running we need to know which iteration are we currently on as well as no. of extracted tokens out of total
                print(f"{i + 1} / {num_iterations} - {len(df_final)} / {total_count} tokens extracted", end='\r')


            # Sometimes we dont get access to extract data; in that case we will know
            else:
                print(f"Error: Unable to fetch data. Status code: {response.status_code}")
                break

        # Storing the final dataframe in the dictionary using the collection_slug as the key
        dfs_dict[collection_slug] = df_final

        # Saving the DataFrame as a CSV file; distinguishing based on collection_slug
        csv_filename = f"{collection_slug}_tokens.csv"
        df_final.to_csv(csv_filename, index=False)
        print(f"Saved CSV file for collection_slug '{collection_slug}' as {csv_filename}")



    # Sometimes we dont get access to extract data; in that case we will know
    else:
        print(f"Error: Unable to fetch initial data. Status code: {initial_response.status_code}")

# Displaying the dictionary of DataFrames
for key, value in dfs_dict.items():
    print(f"DataFrame for collection_slug '{key}':\n{value}")
