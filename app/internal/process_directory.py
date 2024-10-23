import pandas as pd
import numpy as np
from datetime import datetime, date
import os
import re
from config import settings
from itertools import islice
from pydantic import BaseModel, ValidationError
from typing import List, Optional

class CamMember(BaseModel):
    name: Optional[str]
    mobile: Optional[str]
    email: Optional[str]
    birth: Optional[str]
    age: Optional[str]
    byear: Optional[int]
    bmonth: Optional[int]
    gender: Optional[str]
    work: Optional[str]
    gender: Optional[str]
    marriage: Optional[str]
    title: Optional[str]

def process_directory(directory_path):
    csv_files = sorted([file for file in os.listdir(directory_path) if file.endswith('.csv')])

    df1 = pd.read_csv(os.path.join(directory_path, csv_files[0]), on_bad_lines='skip')
    df2 = pd.read_csv(os.path.join(directory_path, csv_files[1]), on_bad_lines='skip')
    event_name = directory_path.split('/')[-1]

    df2 = df2.drop_duplicates(subset=['record_id', 'meta_key'], keep='first')
    df2 = df2.pivot(index='record_id', columns='meta_key', values='meta_value')
    df2.reset_index(inplace=True)
    df2 = df2.rename(columns={'record_id': 'id'})

    df1['id'] = df1['id'].astype(str)
    df2['id'] = df2['id'].astype(str)

    merged_df = pd.merge(df1, df2, on='id', how='outer')

    required_columns = ['name', 'mobile', 'email', 'birth', 'age', 'byear', 'bmonth', 'work', 'gender', 'marriage', 'title']
    target_columns = []
    for column in required_columns:
        if column in merged_df:
            target_columns.append(column)
    final_df = merged_df[target_columns]
    final_df['event'] = event_name

    return final_df

def getSheMember():
    she_member = pd.read_csv(settings.DATA_DIR+'SheMember.csv',  on_bad_lines='skip')
    she_member.rename(columns={'birth_month': 'bmonth', 'birth_year': 'byear', 'phone': 'mobile'}, inplace=True)
    she_member['name'] = she_member['first_name'] + ' ' + she_member['last_name']
    return she_member[['name', 'mobile', 'email', 'byear', 'bmonth', 'gender']]


def combine_directories():
    result = []
    parent_directory = settings.DATA_DIR
    subdirectories = [os.path.join(parent_directory, d)
        for d in os.listdir(parent_directory)
        if os.path.isdir(os.path.join(parent_directory, d))]
    for subdir in subdirectories:
        result.append(process_directory(subdir))  # Corrected this line
    print(result)  # Print the result after appending
    combined = pd.concat(result, ignore_index=True)


    combined['age'] = combined['age'].str.replace(r'[^\d\-,]', '', regex=True)
    def calculate_midpoint(value, offset = 2):
        match = re.match(r'^(\d+)-(\d+)$', str(value))
        if match:
            start, end = map(int, match.groups())
            return  datetime.now().year - ((start + end) // 2 + offset)
        match = re.match(r'^(\d+)$', str(value))
        if match:
            return  datetime.now().year - int(value)
        return value
    combined['age'] = combined['age'].apply(calculate_midpoint)

    def map_title(title):
        title_map = {
            'MS': 'F',
            '小姐': 'F',
            '太太': 'F',
            'MR': 'M',
            '先生': 'M',
            'MRS': 'F'
        }
        return title_map.get(title, title)
    combined['title'] = combined['title'].map(map_title)


#     combined.to_csv('campaign_data.csv', index=False)
    she_member = getSheMember()
    combined = pd.concat([combined, she_member], ignore_index=True)

    # Print column names for debugging
    print("Column names:")
    print(combined.columns.tolist())

    members = []
    for _, row in combined.head(100)[['name', 'mobile', 'email', 'birth', 'age', 'byear', 'bmonth', 'work', 'gender', 'marriage', 'title']].iterrows():
        # Print name value for debugging
        print(f"Name value: {row['name']}")

        # Handle NaN values
        name = str(row['name']) if pd.notna(row['name']) else None
        gender = str(row['gender']) if pd.notna(row['gender']) else None
        mobile = str(row['mobile']) if pd.notna(row['mobile']) else None
        email = str(row['email']) if pd.notna(row['email']) else None
        work = str(row['work']) if pd.notna(row['work']) else None
        gender = str(row['gender']) if pd.notna(row['gender']) else None
        marriage = str(row['marriage']) if pd.notna(row['marriage']) else None
        title = str(row['title']) if pd.notna(row['title']) else None
        birth = str(row['birth']) if pd.notna(row['birth']) else None
        age = str(row['age']) if pd.notna(row['age']) else None

        # Convert byear and bmonth to integers, handling NaN values
        byear = int(row['byear']) if pd.notna(row['byear']) else None
        bmonth = int(row['bmonth']) if pd.notna(row['bmonth']) else None


        try:
            member = CamMember(
                name = name,
                gender = gender,
                mobile = mobile,
                email = email,
                work = work,
                marriage = marriage,
                title = title,
                byear = byear,
                bmonth = bmonth,
                birth = birth,
                age = age,
            )
            members.append(member)
        except ValidationError as e:
            print(f"Validation error for row: {row}")
            print(f"Error details: {e}")
    return members

# Export functions
__all__ = ["process_directory", "getSheMember","combine_directories"]
