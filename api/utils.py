import pandas as pd
from os import path
from flask import send_file
from io import TextIOWrapper, BytesIO
from csv_diff import load_csv, compare
from typing import Callable


def filename(file, default=''):
    return path.splitext(path.basename(
        getattr(file, 'filename') or default))[0]


def bufferize(df, ext='csv'):
    buffer = BytesIO()
    if ext == 'xlsx':
        df.to_excel(buffer, encoding='utf-8')
    else:
        df.to_csv(buffer, index=False, encoding='utf-8')
    buffer.seek(0)
    return buffer


def extension(download_name):
    return path.splitext(download_name)[1][1:]


mimetype = {
    'csv': 'text/csv',
    'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
}


def export(df, download_name):
    ext = extension(download_name) or 'csv'
    return send_file(bufferize(df, ext), download_name=download_name,
                     as_attachment=True, mimetype=mimetype[ext])


def hash(df, cols):
    return df[cols].fillna('').astype(str).sum(axis=1)


def dedup(df, index):
    """
    concat duplicate column values when grouped by index
    """
    # split -> str groupby smaller subset -> merge back for performance
    duplicated = df.duplicated(subset=index, keep=False)
    dupes = df[duplicated]
    if dupes.empty:
        return df
    nodupes = dupes.groupby(index, dropna=False).agg(
        lambda x: ' | '.join(x.dropna().astype(str).str.lower().drop_duplicates()
                             .sort_values())).reset_index()
    return pd.concat([df[~duplicated], nodupes])


def condo_coop_mask(dfc):
    condo_coop = dfc['ContactDescription'].str.upper().isin([
        'CONDO', 'CO-OP'])
    return condo_coop


def prepare_contacts(contacts, index, old=False):
    dfc = contacts
    # extract relevant subset
    if not old:
        dfc = dfc[condo_coop_mask(dfc)]

    dfc = dfc.drop_duplicates()
    # concat values for duplicate index rows
    dfc = dedup(dfc, index)
    dfc = dfc.set_index(pd.Index(hash(dfc, index), name='hash'))

    return dfc


def index_changes(odf, ndf):
    index_name = ndf.index.name or 'index'
    changes = pd.DataFrame(
        ndf.index,
        columns=[index_name]).merge(
        pd.DataFrame(
            odf.index,
            columns=[index_name]),
        on=index_name,
        how='outer',
        indicator=True)
    changes = changes.rename(columns={'_merge': 'ChangeType'})
    changes['ChangeType'] = changes['ChangeType'].map(
        {'left_only': 'added', 'both': 'changed', 'right_only': 'removed'})
    return changes.set_index(index_name)


def diff_frames(odf: pd.DataFrame, ndf: pd.DataFrame,
                ignore_cols:list[str] = [], show_atleast: list[str] = [],
                removed_mask: Callable[[pd.DataFrame], pd.Series] = None):
    """
    Compare dataframes with identical columns on index

    Parameters
    ----------
    odf          : old dataframe
    ndf          : new dataframe
    ignore_cols  : columns to ignore from comparison
    show_atleast : show atleast these columns even when no change
    removed_mask : accepts removed rows as dataframe and
        retuns identically indexed boolean mask for rows to keep

    """
    cdf = index_changes(odf, ndf)

    removed = cdf['ChangeType'] == 'removed'
    if removed_mask:
        removed = removed_mask(odf[removed])
        removed = pd.Series(removed, odf.index).fillna(False)
    removed = odf[removed]

    added = ndf[cdf['ChangeType'] == 'added']

    changed = cdf['ChangeType'] == 'changed'
    changed = changed.index[changed]
    subset = ndf.columns.difference(ignore_cols)
    changed = odf.loc[changed, subset].compare(
        ndf.loc[changed, subset]).convert_dtypes()

    changed.columns = changed.columns.set_levels(['new', 'old'], level=1)

    changed_cols = changed.columns.levels[0]

    for col in show_atleast:
        if col not in changed_cols:
            changed[col] = ndf.loc[changed.index, col]

    # convert to multiindex for concatenation
    added.columns = added.columns.map(
        lambda c:
        (c, 'new') if c in changed_cols else (c, ''))
    removed.columns = removed.columns.map(
        lambda c:
        (c, 'old') if c in changed_cols else (c, ''))

    combined = pd.concat([changed, added, removed]).dropna(
        how='all', axis=1)
    combined.columns = combined.columns.remove_unused_levels()

    combined['ChangeType'] = cdf.loc[combined.index, 'ChangeType']

    return combined


def post_process_contacts(contacts, buildings, old_rids, col_order={}):
    dfc = contacts

    # detect new-buildings
    added = dfc['ChangeType'] == 'added'
    new_rids = ~dfc.loc[added, 'RegistrationID'].isin(old_rids)
    dfc['ChangeType'] = dfc['ChangeType'].cat.add_categories('new-building')
    dfc.loc[pd.Series(new_rids, dfc.index).fillna(
        False), 'ChangeType'] = 'new-building'

    # merge buildings info
    buildings.columns = buildings.columns.map(lambda c: (c, ''))
    dfc = dfc.merge(buildings, on='RegistrationID', how='left')

    # zip match
    zip_match_mask = None
    if ('BusinessZip', '') in dfc.columns:
        zip_match_mask = dfc['BusinessZip'] == dfc['Zip']
    else:
        zip_match_mask = (dfc[('BusinessZip', 'new')] == dfc['Zip']) | (
            dfc[('BusinessZip', 'old')] == dfc['Zip'])
    dfc.loc[zip_match_mask, 'ZipMatch'] = 'Y'

    # rearrange columns
    first = col_order.get('first') or []
    last = col_order.get('last') or []
    columns = first + [*dfc.columns.levels[0].difference(first + last)] + last
    dfc = dfc.reindex(columns=columns, level=0)

    return dfc


def diff(old_file, new_file, index=None, show_atleast=None):
    if not show_atleast and index:
        show_atleast = [index]
    difference = compare(load_csv(TextIOWrapper(old_file), key=index),
                         load_csv(TextIOWrapper(new_file), key=index),
                         show_unchanged=bool(show_atleast))
    changed = list(
        map(lambda row:
            {
                'ChangeType': 'changed',
                **{col: row['unchanged'][col] for col in show_atleast},
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
