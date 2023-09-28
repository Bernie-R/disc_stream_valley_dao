import streamlit as st
import pandas as pd
import requests
import datetime
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt
from main_2 import get_weekly_message_counts, get_channels, fetch_messages
from wordcloud import WordCloud
from dotenv import load_dotenv, find_dotenv
import os

# Load the stored environment variables
load_dotenv()

# Get the values
server_id = os.getenv("server_id")
channel_id = os.getenv("channel_id")
authorization = os.getenv("authorization")


headers = {"authorization": authorization}

st.set_option('deprecation.showPyplotGlobalUse', False)

def plot_message_counts_by_user(df):
    user_message_counts = df['name'].value_counts().head(10)
    
    plt.figure(figsize=(12, 7))
    user_message_counts.plot(kind='bar', color='skyblue')
    
    plt.title('Number of Messages by User (Top 10)')
    plt.xlabel('User')
    plt.ylabel('Number of Messages')
    plt.grid(axis='y', linestyle='--', linewidth=0.7, alpha=0.7)
    
    plt.xticks(rotation=45)
    for label in plt.gca().get_xticklabels():
        label.set_horizontalalignment('right')
    
    plt.tight_layout()

    return plt



def generate_wordcloud(df):
    # Combine all rows in the 'content' column into a single string
    text = " ".join(content for content in df['content'])

    # Generate the word cloud
    wordcloud = WordCloud(background_color="white", width=800, height=400).generate(text)

    # Display the generated image using matplotlib
    plt.figure(figsize=(10, 6))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    
    return plt


def plot_weekly_message_counts(df, weekly_counts):
    plt.figure(figsize=(10, 6))
    for channel in weekly_counts['channel'].unique():
        channel_data = weekly_counts[weekly_counts['channel'] == channel]
        plt.plot(channel_data['Week_Ending'], channel_data['count'], marker='o')

    plt.title('Weekly Message Counts for Selected Channel')
    plt.xlabel('Week Ending')
    plt.ylabel('Message Count')
    plt.grid(True, which='both', linestyle='--', linewidth=0.5)
    plt.tight_layout()
    return plt

def main():
    st.title("Nani?")

    # Sidebar
    st.sidebar.title("Channel Selection")
    channels, names = get_channels()

    name_channel_dict = dict(zip(names, channels))

    selected_channel = st.sidebar.selectbox("Choose a Channel:", names)

    selected_channel = name_channel_dict[selected_channel]

    # Load Data Button
    if st.sidebar.button("Load Data"):
        messages = fetch_messages(selected_channel, headers)
        
        if not messages:
            st.sidebar.text("No messages found.")
            return

        # Extract message details
        data = []
        for message in messages:
            author = message.get('author', {})
            details = {
                'name': author.get('global_name'),
                'username': author.get('username'),
                'timestamp': message.get('timestamp'),
                'content': message.get('content'),
                'channel': message.get("channel_id")
            }
            data.append(details)

        # Convert list of dictionaries to DataFrame
        df = pd.DataFrame(data)
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed')
        df['timestamp'] = df['timestamp'].dt.tz_localize(None)

        # Get weekly message counts for the last year
        one_year_ago = datetime.datetime.now() - relativedelta(years=1)
        weekly_counts = get_weekly_message_counts(df, one_year_ago)
        
        weekly_plot = plot_weekly_message_counts(df, weekly_counts)
        st.pyplot(weekly_plot)

        user_bar, word_cloud = st.columns(2)

        user_bar_plot = plot_message_counts_by_user(df)
        with user_bar:
            st.pyplot(user_bar_plot)

        
        word_cloud_plot = generate_wordcloud(df)
        with word_cloud:
            st.pyplot(word_cloud_plot)

if __name__ == "__main__":
    main()
