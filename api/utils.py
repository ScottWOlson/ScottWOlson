import pandas as pd
from os import path
from flask import send_file
from io import TextIOWrapper, BytesIO
from csv_diff import load_csv, compare
from rapidfuzz import fuzz, process
from rapidfuzz.utils import default_process as rapidfuzz_process
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


def fuzzyfy(df, similarity=90):
    """
    Groupby and sum Count of fuzzyfied names:
    - names are pre-processed using token sort.
    - grouped together based on `similarity` factor.
    - finally, their Count is aggregated as FuzzyCount.

    Complexity:
        Each iteration removes matched names from comparison.
        Let k be the maximum group size for a given similarity,
        and n be the number of names.
        If every group is of size k, we arrive at a lower-bound
        of O(n²/2k). Any other distribution of group sizes performs worse
        with left skewed ones approaching O(n²)

    Existence of a better than quadratic time algorithm:
        Consider sorting as an example: If you know b is less than
        remainder of the list, and a < b, you're wasting cycles comparing
        a with remainder of the list. Hence why, sorting can be optimally
        performed in O(n·log(n)). Similar principle applies here,
        if you can conclusively prove, that checking a particular subset of discarded
        ratios from previous iterations leads to a worse similarity score,
        the time complexity may be substantially improved.
        Indel/Levenshtein edit distancing is a lossy operation by nature
        but, perhaps an upper bound exists to help filter similar ratios
        for successive iterations. With the right ordering of names, exponential decay
        of comparison input size will result in a different function class altogether!

    Parameters
    ----------
    df          : dataframe with Name: string, Count: number columns
    similarity  : number between 0-100; names to be considered within the same group,
                if they are `similarity` percent match

    Returns
    -------
    dataframe with FuzzyName, FuzzyCount, Name, Count columns

    """
    def process_name(s):
        processed = rapidfuzz_process(s)
        return " ".join(sorted(processed.split())) if processed else s

    name_col, count_col = df.columns
    # we got memory but no time! 🏃
    compare = dict(df[name_col].apply(process_name))
    rows = list(df.values)

    for k, row in enumerate(rows):
        # ignore if already processed
        if isinstance(row, tuple):
            continue

        matches = process.extract(
            compare[k],
            compare,
            limit=None,
            processor=None,
            scorer=fuzz.ratio,
            score_cutoff=similarity)

        fuzzy_count = 0
        fuzzy_name = row[0]
        for (_, score, key) in matches:
            name, count = rows[key]
            rows[key] = (pd.NA, pd.NA, name, count, score, k)
            fuzzy_count += count
            compare.pop(key, None)

        # set fuzzy name row
        rows[k] = (fuzzy_name, fuzzy_count) + rows[k][2:]

    def sort(row):
        name, count, score, key = row[2:]
        fuzzy_name, fuzzy_count = rows[key][0:2]
        break_tie_eq_fzname_fzcount = fuzzy_count + 1 if fuzzy_name == name else count
        return (fuzzy_count, fuzzy_name,
                break_tie_eq_fzname_fzcount, score, name)

    return pd.DataFrame(sorted(rows, key=sort, reverse=True), columns=[
                        f'Fuzzy{name_col}', f'Fuzzy{count_col}',
                        name_col, count_col, 'Similarity', 'key']).drop('key', axis=1)


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
        columns=[index_name]
    ).merge(
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
