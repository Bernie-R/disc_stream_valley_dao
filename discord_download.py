import requests
import json
import ast
import numpy as np
import pandas as pd
import datetime
from dateutil import parser
from dotenv import load_dotenv, find_dotenv
import os
import time
#from supabase_connector import supabase

class discord:
    def retrieve_message_data(channel_id, authorization, server_id, last_downloaded_message_id=None):
        discord_epoch = 1443417695
        first_time = True
        messages = {}
        i = 0

        # Create empty dataframe for messages
        messages_df = pd.DataFrame(
            columns=(
                "message_id",
                "user_id",
                "username",
                "channel",
                "timestamp",
                "content",
                "message_link",
                "link",
                "title",
                "description",
            )
        )
        mentions_df = pd.DataFrame(columns=("message_id", "username"))
        reactions_df = pd.DataFrame(columns=("message_id", "username", "reaction"))

        while first_time or len(messages) > 0:
            # If it's the first time through the loop - use now() for Discord API
            if first_time:
                # Calculate Snowflake time
                before = (
                    int(datetime.datetime.now().timestamp() * 1000) - discord_epoch << 22
                )
                first_time = False

            # Otherwise, use the timestamp of the last message in the API results
            else:
                before = last_time - discord_epoch << 22


            # Hit Discord API for message data
            headers = {"authorization": authorization}
            r = requests.get(
                f"https://discord.com/api/v9/channels/{channel_id}/messages?before={before}",
                headers=headers,
            )
            
            if r.status_code != 200:
                print("Error: ", r.status_code)
                print("finished downloading: ", channel_id, messages_df.shape[0])
                return messages_df, mentions_df, reactions_df
            
            messages = json.loads(r.text)

            if len(messages) == 0:
                print(channel_id, messages_df.shape[0])
                return messages_df, mentions_df, reactions_df

            # Parse each message into a row of the dataframe
            for value in messages:
                
                if not isinstance(value, dict):
                    continue
                
                if last_downloaded_message_id is not None and int(value['id']) <= int(last_downloaded_message_id):
                    print("Reached last downloaded message ID")
                    print("finished downloading: ", channel, messages_df.shape[0])
                    return messages_df, mentions_df, reactions_df
                
                # Structure all data
                messages_df.loc[i, "message_id"] = value["id"]
                messages_df.loc[i, "username"] = value["author"]["username"]
                messages_df.loc[i, "user_id"] = value["author"]["id"]
                messages_df.loc[i, "channel"] = channel_id
                messages_df.loc[i, "timestamp"] = value["timestamp"]
                messages_df.loc[i, "content"] = value["content"]
                messages_df.loc[
                    i, "message_link"
                ] = f"https://discord.com/channels/{server_id}/{value['channel_id']}/{value['id']}"
                messages_df.loc[i, "json"] = str(value)

                # Extract link data
                if "http" in value["content"]:
                    try:
                        messages_df.loc[i, "link"] = value["embeds"][type == "link"]["url"]
                        messages_df.loc[i, "title"] = value["embeds"][type == "link"][
                            "title"
                        ]
                        messages_df.loc[i, "description"] = value["embeds"][type == "link"][
                            "description"
                        ]
                    except:
                        continue

                # Extract mentions
                if len(value["mentions"]) > 0:
                    for mention in range(0, len(value["mentions"])):
                        mentions_df = pd.concat(
                            [
                                mentions_df,
                                pd.DataFrame(
                                    {
                                        "message_id": [value["id"]],
                                        "username": [
                                            value["mentions"][mention]["username"]
                                        ],
                                    }
                                ),
                            ]
                        )

                # Extract reactions
                reactions = 0
                reactors_list = []
                if "reactions" in value.keys():
                    for reaction_num in range(0, len(value["reactions"])):
                        reaction = value["reactions"][reaction_num]
                        r = requests.get(
                            f"https://discord.com/api/v9/channels/{channel_id}/messages/{value['id']}/reactions/{reaction['emoji']['name']}",
                            headers=headers,
                        )

                        try:
                            reactors = json.loads(r.text)
                            for reactor in reactors:
                                reactions_df = pd.concat(
                                    [
                                        reactions_df,
                                        pd.DataFrame(
                                            {
                                                "message_id": [value["id"]],
                                                "username": [reactor["username"]],
                                                "reaction": [reaction["emoji"]["name"]],
                                            }
                                        ),
                                    ]
                                )
                            time.sleep(0.5)
                        except:
                            time.sleep(0.5)
                            continue
                i += 1

            # Calculate the Snowflake time for last message in the query result
            last_time = (
                int(parser.parse(messages[len(messages) - 1]["timestamp"]).timestamp())
                * 1000
            )

        return messages_df, mentions_df, reactions_df

    # Return a dataframe of user_ids and roles
    def get_roles(all_messages, authorization, server_id):
        
        # Get role IDs for each user
        all_roles = pd.DataFrame(columns=("username", "role_id", "user_id"))
        user_ids = all_messages.user_id.drop_duplicates().to_list()
        for user_id in user_ids:
            
            url = f"https://discord.com/api/v9/guilds/{server_id}/members/{user_id}"
            headers = {"authorization": f"{authorization}"}
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                roles = pd.DataFrame(response.json()['roles'], columns=['role_id'])
                roles['username'] = response.json()['user']['username']
                all_roles = pd.concat([all_roles, roles])
            else:
                continue

        # Get role names from IDs
        headers = {"authorization": f"{authorization}"}
        response = requests.get(f'https://discord.com/api/guilds/{server_id}/roles', headers=headers)
        roles_df = pd.DataFrame(columns=("role_name", "role_id"))
        if response.status_code == 200:
            roles = response.json()
            for role in roles:
                if role['id'] not in roles_df.role_id:
                    roles_df = pd.concat([roles_df, pd.DataFrame({'role_id':[role['id']], 'role_name':[role['name']]})])
        all_roles = all_roles.merge(roles_df)
        
        if len(all_roles) > 0:
            all_roles = all_roles.groupby(['user_id','username']).agg({'role_name':lambda x: list(x)}).reset_index()
        
        all_roles.rename(columns={'role_name':'roles'}, inplace=True)
        print(all_roles)
        return all_roles


    # Get members' avatars
    def get_avatars(all_messages, authorization, server_id):
        # Get members' avatars
        avatars = (all_messages[['username','timestamp','user_id','json']].sort_values(by = 'timestamp', ascending = False).groupby('username').head(1))
        for i, row in avatars.iterrows():
            avatars.loc[i,'avatar'] = f"https://cdn.discordapp.com/avatars/{row.user_id}/{ast.literal_eval(row.json)['author']['avatar']}.png"

        avatars = avatars[['user_id','avatar']]

        return avatars

    def supabase_add(df, supabase, table_name):

        # Reset the DataFrame's index
        df.reset_index(drop=True, inplace=True)

        # Replace NaN with None
        df = df.where(pd.notna(df), None)

        # Convert dataframe to list of dictionaries
        data = df.to_dict(orient='records')

        # Add data to Supabase
        try:
            supabase().table(table_name).upsert(data).execute()
            print(f"Added {len(data)} rows to {table_name} table")
        except Exception as e:
            print(f"An error occurred: {e}")
            return

        return

    def download_messages_since(snowflake):
        # Start Here
        authorization = os.environ.get("DISCORD_API_KEY")
        server_id = int(os.environ.get("DISCORD_SERVER_ID"))

        for i, channel in pd.read_csv("etl/channels.csv").iterrows():

            # # Testing filter - REMOVE
            # if channel.channel != 'wg-applicants':
            #     continue
            
            print(channel.channel)

            ###############
            ## DOWNLOAD ###
            ###############
            
            # # Start downloading messages from last downloaded timestamp
            messages, mentions, reactions = retrieve_message_data(
                channel.channel, channel.channel_id, authorization, server_id, last_downloaded_message_id=snowflake
            )

            if len(messages) == 0:
                continue

            # Get new user roles
            all_roles = get_roles(messages, authorization, server_id)
            print('all_roles: ', all_roles)
                    
            # Get new user avatars
            all_avatars = get_avatars(messages, authorization, server_id)
            print('all_avatars: ', all_avatars)

            # combine roles and avatars into one dataframe with user_id as the key
            users = all_roles.merge(all_avatars, on='user_id', how='left')

            ###############
            ## DB UPDATE ##
            ###############
            
            # remove the json column from messages
            messages.drop(columns=['json'], inplace=True)

            # Add new users to supabase
            supabase_add(users, supabase, 'users')
            
            # Add messages, mentions, and reactions to Supabase
            supabase_add(messages, supabase, 'messages')
            supabase_add(mentions, supabase, 'mentions')
            supabase_add(reactions, supabase, 'reactions')

            time.sleep(5)
            
            
    # download_messages_since(1137078384011264123)
        