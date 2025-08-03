#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO

def matches(data):
    match_rows = []
    for match in data['matches']:
    # Match record
        match_rows.append({
            'match_id': match['id'],
            'utc_date': match['utcDate'],
            'status': match['status'],
            'matchday': match['matchday'],
            'stage': match['stage'],
            'group': match['group'],
            'last_updated': match['lastUpdated'],
            'duration': match['score']['duration'],
            'winner': match['score']['winner'],
            'season_id': match['season']['id'],
            'competition_id': match['competition']['id'],
            'home_team_id': match['homeTeam']['id'],
            'away_team_id': match['awayTeam']['id']
        })
        return match_rows

def teams(data):
    team_rows = []
    for match in data['matches']:
        team_rows.extend([
        match['homeTeam'],
        match['awayTeam']
    ])
    return team_rows

def season(data):
    season_rows = []
    for match in data['matches']:
        season_rows.append(match['season'])
        return season_rows

def score(data):
    score_rows = []
    for match in data['matches']:
        full_time = match['score']['fullTime']
        half_time = match['score']['halfTime']

        score_rows.append({
        'match_id': match['id'],
        'full_time_home': full_time['home'],
        'full_time_away': full_time['away'],
        'half_time_home': half_time['home'],
        'half_time_away': half_time['away']
    })
    return score_rows

def referee(data):
    referee_rows = []
    for match in data['matches']:
        for ref in match['referees']:
            referee_rows.append({
            'match_id': match['id'],
            'referee_id': ref.get('id'),
            'name': ref.get('name'),
            'nationality': ref.get('nationality'),
            'role': ref.get('role')
        })
        return referee_rows

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = 'pl-etl-ishan-project'
    key = 'raw_data/to_process/'

    pl_data = []
    pl_key = []
    s3.list_objects(Bucket=Bucket,Prefix = key)['Contents']
    for file in s3.list_objects(Bucket=Bucket, Prefix = key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == 'json':
            response = s3.get_object(Bucket=Bucket, Key=file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            pl_data.append(jsonObject)
            pl_key.append(file_key)
    
    for data in pl_data:
        match_rows = matches(data)
        team_rows = teams(data)
        season_rows = season(data)
        score_rows = score(data)
        referee_rows = referee(data)

    # Convert to DataFrames
    matches_df = pd.DataFrame(match_rows)
    team_df = pd.DataFrame(team_rows)
    season_df = pd.DataFrame(season_rows)
    score_df = pd.DataFrame(score_rows)
    referee_df = pd.DataFrame(referee_rows)

    #Rename ID column in df for clarity
    team_df.rename(columns={'id': 'team_id'}, inplace=True)
    season_df.rename(columns={'id': 'season_id'}, inplace=True)

    #Drop duplicates
    matches_df = matches_df.drop_duplicates(subset = 'match_id')
    team_df = team_df.drop_duplicates(subset = 'team_id')
    season_df = season_df.drop_duplicates(subset = 'season_id')

    #To Datetime
    matches_df["utc_date"]= pd.to_datetime(matches_df["utc_date"])
    matches_df["last_updated"]= pd.to_datetime(matches_df["last_updated"])
    season_df["startDate"] = pd.to_datetime(season_df["startDate"])
    season_df["endDate"] = pd.to_datetime(season_df["endDate"])

    matches_key = "transformed_data/pl_matches/" + str(datetime.now()) + ".csv"
    matches_buffer = StringIO()
    matches_df.to_csv(matches_buffer, index=False)
    matches_content = matches_buffer.getvalue()
    s3.put_object(Bucket=Bucket, Key=matches_key, Body=matches_content)

    team_csv = "transformed_data/pl_teams/" + str(datetime.now()) + ".csv"
    team_buffer = StringIO()
    team_df.to_csv(team_buffer, index=False)
    team_content = team_buffer.getvalue()
    s3.put_object(Bucket=Bucket, Key=team_csv, Body=team_content)

    season_csv = "transformed_data/pl_season/" + str(datetime.now()) + ".csv"
    season_buffer = StringIO()
    season_df.to_csv(season_buffer, index=False)
    season_content = season_buffer.getvalue()
    s3.put_object(Bucket=Bucket, Key=season_csv, Body=season_content)

    score_csv = "transformed_data/pl_score/" + str(datetime.now()) + ".csv"
    score_buffer = StringIO()
    score_df.to_csv(score_buffer, index=False)
    score_content = score_buffer.getvalue()
    s3.put_object(Bucket=Bucket, Key=score_csv, Body=score_content)

    referee_csv = "transformed_data/pl_referee/" + str(datetime.now()) + ".csv"
    referee_buffer = StringIO()
    referee_df.to_csv(referee_buffer, index=False)
    referee_content = referee_buffer.getvalue()
    s3.put_object(Bucket=Bucket, Key=referee_csv, Body=referee_content)

    s3.resource = boto3.resource('s3')
    for key in pl_key:
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        s3.resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split('/')[-1])
        s3.delete_object(Bucket=Bucket, Key=key)


       

