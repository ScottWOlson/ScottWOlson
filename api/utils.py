import pandas as pd
from os import path
from flask import send_file
from io import TextIOWrapper, BytesIO
from csv_diff import load_csv, compare
from thefuzz import fuzz, process
from typing import Callable


def read_csv(file, default_dtype=None, dtype=None, lower_case=False, **kwargs):
    if default_dtype:
        header = pd.read_csv(file, nrows=0)
        file.seek(0)
        dtype = dict.fromkeys(
            header.columns.difference(
                dtype.keys()), default_dtype)

    df = pd.read_csv(file, dtype=dtype, **kwargs)

    if lower_case:
        df = df.apply(
            lambda col: col.astype('string').str.lower() if (
                col.dtype == 'string') or (
                col.dtype == 'object') else col)

    return df


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


def fuzzyfy(df, likeness=90):
    """
    Grouby and sum Count of fuzzyfied names:
    - names are pre-processed using token sort.
    - grouped together based on `likeness` factor.
    - finally, their Count is aggregated as FuzzyCount.

    Complexity:
        Each iteration removes matched names from comparison.
        Let k be the maximum group size for a given likeness,
        and n be the number of names.
        If every group is of size k, we arrive at a lower-bound
        of O(n²/2k). Any other distribution of group sizes performs worse
        with left skewed ones approaching O(n²)

    Parameters
    ----------
    df        : dataframe with Name: string, Count: number columns
    likeness  : between 0-100; names to be considered within the same group,
                if they are `likeness` percent similar

    Returns
    -------
    dataframe with FuzzyName, FuzzyCount, Name, Count columns

    """
    def process_name(s):
        processed = fuzz._process_and_sort(s, True)
        return processed if processed else s

    name_col = df.columns[0]
    # we got memory but no time! 🏃
    compare = dict(df[name_col].apply(process_name))
    values = list(df.values)
    for k, val in enumerate(values):
        # ignore if already processed
        if isinstance(val, tuple):
            continue
        matches = process.extractBests(
            compare[k],
            compare,
            processor=None,
            scorer=fuzz.ratio,
            score_cutoff=likeness,
            limit=None)
        fuzzy_count = 0
        fuzzy_name = val[0]
        for (_, _, key) in matches:
            fuzzy_name = val[0]
            name = values[key][0]
            count = values[key][1]
            values[key] = (k, pd.NA, pd.NA, name, count)

            fuzzy_count += count
            compare.pop(key, None)

        values[k] = (
            k,
            fuzzy_name,
            fuzzy_count,
            values[k][3],
            values[k][4])

    def sort(val):
        key = val[0]
        name = val[3]
        count = val[4]
        fuzzy_name = values[key][1]
        fuzzy_count = values[key][2]
        break_tie_eq_fzname_fzcount = fuzzy_count + 1 if fuzzy_name == name else count
        return (fuzzy_count, fuzzy_name, break_tie_eq_fzname_fzcount, name)

    return pd.DataFrame(sorted(values, key=sort, reverse=True), columns=[
                        'key',
                        'FuzzyCorpName', 'FuzzyCount',
                        'CorporationName', 'Count']).drop('key', axis=1)


def hash(df, cols):
    return df[cols].fillna('').astype('string').sum(axis=1)


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


def condo_coop_mask(df):
    condo_coop = df['ContactDescription'].str.lower().isin([
        'condo', 'co-op'])
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
                ignore_cols: list[str] = [], show_atleast: list[str] = [],
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


def post_process_contacts(contacts: pd.DataFrame,
                          buildings, old_rids, col_order={}):
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
        zip_match_mask = dfc[('BusinessZip', 'new')] == dfc['Zip']
    dfc.loc[zip_match_mask, 'ZipMatch'] = 'Y'

    # rearrange columns
    first = col_order.get('first') or []
    last = col_order.get('last') or []
    columns = first + [*dfc.columns.levels[0].difference(first + last)] + last
    dfc = dfc.reindex(columns=columns, level=0, copy=False)

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
