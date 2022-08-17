import pandas as pd
from os import path
from flask import send_file
from io import TextIOWrapper, BytesIO
from csv_diff import load_csv, compare


def filename(file, default=''):
    return path.splitext(path.basename(
        getattr(file, 'filename') or default))[0]


def bufferize(df, index=False):
    buffer = BytesIO()
    df.to_csv(buffer, index=index, encoding='utf-8')
    buffer.seek(0)
    return buffer


def export(df, download_name):
    return send_file(bufferize(df), download_name=download_name,
                     as_attachment=True, mimetype='text/csv')


def hash(df, cols):
    return pd.Index(df[cols].fillna('').astype(str).sum(axis=1), name='hash')


# concat duplicate column values when grouped by cols
def dedup(df, cols):
    return df if not df.shape[0] else df.groupby(cols, dropna=False).agg(
        lambda x: ' | '.join(x.dropna().astype(str).str.lower().drop_duplicates()
                             .sort_values())).reset_index()


def parse_contacts(contacts, buildings):
    dfc = pd.read_csv(contacts)
    # extract relevant subset
    dfc = dfc[dfc['ContactDescription'].str.upper().isin(
        ['CO-OP', 'CONDO'])].drop_duplicates()

    cols = ['RegistrationContactID', 'RegistrationID',
            'BusinessZip', 'FirstName', 'MiddleInitial', 'LastName']

    # split -> str groupby smaller subset -> merge back for performance
    duplicated = dfc.duplicated(subset=cols, keep=False)
    dfc = pd.concat([dfc[~duplicated], dedup(dfc[duplicated], cols)])

    dfb = pd.read_csv(buildings)
    dfc = dfc.merge(dfb, on='RegistrationID', how='left')

    # because merge will add rows since RegistrationID repeats in buildings
    cols.append('BuildingID')
    dfc = dfc.set_index(hash(dfc, cols))

    return bufferize(dfc, index=True)


def diff(old_file, new_file, index, keep_cols):
    difference = compare(load_csv(TextIOWrapper(old_file), key=index),
                         load_csv(TextIOWrapper(new_file), key=index),
                         show_unchanged=True)
    changed = list(
        map(lambda row:
            {
                'ChangeType': 'changed',
                **{col: row['unchanged'][col] for col in keep_cols},
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
