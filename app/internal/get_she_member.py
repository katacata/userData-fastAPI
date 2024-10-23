import pandas as pd
import numpy as np
from datetime import datetime, date
import os
import re
from config import settings
from itertools import islice
from pydantic import BaseModel, ValidationError
from typing import List, Optional

class SheMember(BaseModel):
    name: Optional[str]
    mobile: Optional[str]
    email: Optional[str]
    byear: Optional[int]
    bmonth: Optional[int]
    gender: Optional[str]

def get_she_member():
    she_member = pd.read_csv(settings.DATA_DIR+'SheMember.csv', on_bad_lines='skip', encoding='utf-8')
    she_member.rename(columns={'birth_month': 'bmonth', 'birth_year': 'byear', 'phone': 'mobile'}, inplace=True)

    # Ensure 'name' column exists
    if 'first_name' not in she_member.columns or 'last_name' not in she_member.columns:
        she_member['name'] = pd.NA
    else:
        she_member['first_name'] = she_member['first_name'].fillna('')
        she_member['last_name'] = she_member['last_name'].fillna('')
        she_member['name'] = she_member.apply(lambda row: f"{row['first_name']} {row['last_name']}".strip(), axis=1)
        she_member.loc[she_member['name'] == '', 'name'] = pd.NA

    # Print column names for debugging
    print("Column names:")
    print(she_member.columns.tolist())

    members = []
    for _, row in she_member[['name', 'mobile', 'email', 'byear', 'bmonth', 'gender']].iterrows():
        # Print name value for debugging
        print(f"Name value: {row['name']}")

        # Handle NaN values
        name = str(row['name']) if pd.notna(row['name']) else None
        gender = str(row['gender']) if pd.notna(row['gender']) else None
        mobile = str(row['mobile']) if pd.notna(row['mobile']) else None
        email = str(row['email']) if pd.notna(row['email']) else ''

        # Convert byear and bmonth to integers, handling NaN values
        byear = int(row['byear']) if pd.notna(row['byear']) else None
        bmonth = int(row['bmonth']) if pd.notna(row['bmonth']) else None

        try:
            member = SheMember(
                name=name,
                mobile=mobile,
                email=email,
                byear=byear,
                bmonth=bmonth,
                gender=gender
            )
            members.append(member)
        except ValidationError as e:
            print(f"Validation error for row: {row}")
            print(f"Error details: {e}")

    return members

__all__ = ["get_she_member"]
