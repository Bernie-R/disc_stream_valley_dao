import os

import pandas as pd
from dotenv import load_dotenv, find_dotenv
from discord_download import discord
import requests
import json
import re
import datetime
from dateutil.relativedelta import relativedelta
import time
import matplotlib.pyplot as plt

# Load the stored environment variables
load_dotenv()

# Get the values
server_id = os.getenv("server_id")
channel_id = os.getenv("channel_id")
authorization = os.getenv("authorization")

headers = {"authorization": authorization}

def get_channels():

    response = requests.get(f'https://discord.com/api/v10/guilds/{server_id}/channels', headers=headers)

    channel_id = []
    name = []
    if response.status_code == 200:
        guilds = response.json()
        for guild in guilds:
            channel_id.append(guild['id'])
            name.append(guild['name'])


    return channel_id, name


def get_weekly_message_counts(messages_df, start_date):
    # Convert the timestamp column to datetime type
    print(messages_df)
    messages_df['timestamp'] = pd.to_datetime(messages_df['timestamp'])

    # Set the timestamp as the index
    messages_df.set_index('timestamp', inplace=True)

    # Group by week and channel and count the number of messages
    weekly_counts = messages_df.groupby([pd.Grouper(freq='W'), 'channel']).size().reset_index()
    weekly_counts.columns = ['Week_Ending', 'channel', 'count']

    # Filter only for the weeks after the given start date
    weekly_counts = weekly_counts[weekly_counts['Week_Ending'] > start_date]

    return weekly_counts

def fetch_messages(CHANNEL_ID, HEADERS, before_id=None):
    BASE_URL = f'https://discord.com/api/v10/channels/{CHANNEL_ID}/messages'
    params = {
        'limit': 100
    }
    all_messages = []

    while True:
        if before_id:
            params['before'] = before_id

        response = requests.get(BASE_URL, headers=HEADERS, params=params)
        # Error handling (rudimentary)
        if response.status_code != 200:
            print(f"Error {response.status_code}: {response.text}")
            break
        
        batch = response.json()
        print(batch)
        if not batch:  # No more messages
            break

        all_messages.extend(batch)
        
        # Check for the end of messages
        if len(batch) < 100:  
            break
        
        # Update the before_id for the next batch
        before_id = batch[-1]['id']
        print(before_id)

    return all_messages
