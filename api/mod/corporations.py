import pandas as pd
from ..utils.common import (
    condo_coop_mask,
    not_contains_mask,
)


def prepare(
        contacts_file, buildings_file,
        building_cols, filter_keywords):
    dtype = {
        'RegistrationID': 'UInt32',
        'CorporationName': 'string',
        'ContactDescription': 'string'}
    df = pd.read_csv(
        contacts_file,
        usecols=dtype.keys(),
        dtype=dtype).dropna(
        subset='CorporationName')

    # extract relevant subset
    df = df[condo_coop_mask(df)]
    df = df[not_contains_mask(df['CorporationName'], filter_keywords,
                              regex=False, case=False)]
    df = df[['RegistrationID', 'CorporationName']].drop_duplicates()

    dfbs = None
    if building_cols:
        buildings = pd.read_csv(
            buildings_file,
            usecols=building_cols + ['RegistrationID'],
            dtype='UInt32')
        dfbs = df.merge(
            buildings,
            on='RegistrationID').drop(
            'RegistrationID',
            axis=1)
        dfbs = dfbs.groupby('CorporationName').sum()

    count = df.groupby('CorporationName').size().rename('Count')
    # sorting has added benefit of optimizing fuzzyfy:
    # assuming names that repeat most often also contain the largest
    # number of spelling errors.
    count = count.sort_values(ascending=False)

    return pd.concat([count, dfbs], axis=1).fillna(0).reset_index()
