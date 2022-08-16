import pandas as pd
from os import path
from flask import send_file
from io import TextIOWrapper, BytesIO
from csv_diff import load_csv, compare


def filename(file, default=''):
    return path.splitext(path.basename(
        getattr(file, 'filename') or default))[0]


def bufferize(df):
    buffer = BytesIO()
    df.to_csv(buffer, index=False, encoding='utf-8')
    buffer.seek(0)
    return buffer


def export(df, download_name):
    return send_file(bufferize(df), download_name=download_name,
                     as_attachment=True, mimetype='text/csv')


def parse_contacts(contacts, buildings):
    dfb = pd.read_csv(buildings)
    dfc = pd.read_csv(contacts)
    dfc = dfc[dfc['ContactDescription'].str.upper().isin(['CO-OP', 'CONDO'])]
    dfc = dfc.merge(dfb, on='RegistrationID', how='left')
    return bufferize(dfc)


def diff(old_file, new_file, index):
    difference = compare(load_csv(TextIOWrapper(old_file), key=index),
                         load_csv(TextIOWrapper(new_file), key=index))
    changed = list(
        map(lambda row:
            {
                'ChangeType': 'changed',
                index: row['key'],
                **{
                    col: ' -> '.join(change) for col, change in row['changes'].items()
                }
            },
            difference['changed']))
    added = list(
        map(lambda row: {**row, 'ChangeType': 'added'}, difference['added']))
    removed = list(
        map(lambda row: {**row, 'ChangeType': 'removed'}, difference['removed']))

    df = pd.DataFrame(changed + added + removed)
    if difference['columns_added']:
        df['ColumnsAdded'] = pd.Series(difference['columns_added'])
    if difference['columns_removed']:
        df['ColumnsRemoved'] = pd.Series(difference['columns_removed'])
    return df
