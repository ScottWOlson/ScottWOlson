from csv_diff import load_csv, compare
from flask import send_file
from io import StringIO
from io import BytesIO
import pandas as pd
from os import path


def filename(file, default=''):
    return path.splitext(path.basename(getattr(file, 'filename', default)))[0]


def export(df, download_name):
    buffer = BytesIO()
    df.to_csv(buffer, index=False, encoding='utf-8')
    buffer.seek(0)
    return send_file(buffer, download_name=download_name,
                     as_attachment=True, mimetype='text/csv')


def stringify(df):
    buffer = StringIO()
    df.to_csv(buffer, index=False, encoding='utf-8')
    buffer.seek(0)
    return buffer


def load_compare_df(buffer, index):
    df = pd.read_csv(buffer).drop_duplicates()
    if index == 'RegistrationContactID':
        df = df[df['ContactDescription'].str.upper().isin(['CO-OP', 'CONDO'])]
    return df


def diff(df1, df2, index):
    difference = compare(load_csv(stringify(df1), key=index),
                         load_csv(stringify(df2), key=index))
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
