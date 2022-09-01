import pandas as pd
from os import path
from io import BytesIO
from flask import send_file
from rapidfuzz import fuzz, process
from rapidfuzz.utils import default_process as rapidfuzz_process
from typing import Callable


def lower_case(df):
    def lower(col):
        if col.dtype == 'string':
            return col.str.lower()
        elif col.dtype == 'object':
            return col.map(lambda s: s.lower() if isinstance(
                s, str) else s, na_action='ignore')
        return col
    return df.apply(lower)


def read_csv(file, default_dtype=None, dtype=None, lower_case=False, **kwargs):
    if default_dtype:
        header = pd.read_csv(file, nrows=0)
        file.seek(0)
        dtypes = dtype or {}
        dtype = {
            **dict.fromkeys(
                header.columns.difference(
                    dtypes.keys()), default_dtype),
            **dtypes
        }

    df = pd.read_csv(file, dtype=dtype, **kwargs)

    if lower_case:
        df = lower_case(df)

    return df


def parse_list(string):
    return string.split(',') if string else []


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


def condo_coop_mask(df):
    condo_coop = df['ContactDescription'].str.lower().isin([
        'condo', 'co-op'])
    return condo_coop


def not_contains_mask(series, keywords=[], **contains_args):
    mask = True
    for word in keywords:
        mask &= ~series.str.contains(word, **contains_args)
    return mask


def prepare_corporation_count(
        contacts_file, buildings_file,
        building_cols, filter_keywords):
    dtype = {
        'RegistrationID': 'UInt32',
        'CorporationName': 'string',
        'ContactDescription': 'string'}
    df = read_csv(
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
        buildings = read_csv(
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

    return pd.concat((count, dfbs), axis=1).fillna(0).reset_index()


def fuzzyfy(df, similarity=90, ignore_keywords=[]):
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
        for successive iterations. With the right ordering of names,
        initial-size-dependent decay will result in a different function class altogether!
        Specifically, for a given distribution, we generate the maximally decaying curve Ψ(i, n),
        where Ψ is the comparison input size at ith iteration starting with n names.
        Then, the time complexity can be expressed as Ω(integral(0, n, Ψ(i,n)·di)).

    Parameters
    ----------
    df               : dataframe with Name: string, Count: number, *ExtraColumns: number columns
    similarity       : number between 0-100; names to be considered within the same group,
        if they are `similarity` percent match
    ignore_keywords  : keywords that get pre-processed into empty string before ratio computation.

    Returns
    -------
    dataframe with FuzzyName, FuzzyCount, *FuzzySumExtraColumns,
                   Name, Count, *ExtraColumns, Similarity columns

    """
    def process_name(s: str):
        processed = rapidfuzz_process(s)
        if processed:
            # faster than regex
            for word in ignore_keywords:
                processed = processed.replace(word, '')
            return processed
        return s

    size = len(df.columns)
    pd_nas = (pd.NA,) * size
    # we got memory but no time! 🏃
    compare = dict(df[df.columns[0]].apply(process_name))
    rows = list(df.values)
    for k, row in enumerate(rows):
        # ignore if already processed
        if len(row) > size:
            continue

        matches = process.extract(
            compare[k],
            compare,
            limit=None,
            processor=None,
            score_cutoff=similarity,
            scorer=fuzz.token_set_ratio)

        # Steering away from dataframe indexing required when groupby.agg was used,
        # provided considerable performance benefit. Now with rapidfuzz,
        # we may switch back to pandas aggregation to improve readability.
        # Although, customized sorting is still best performed on df.values.
        fuzzy_name = row[0]
        fuzzy_sums = [0] * (size - 1)
        for (_, score, key) in matches:
            _, *values = rows[key]
            rows[key] = pd_nas + (*rows[key], score, k)
            for i, val in enumerate(values):
                fuzzy_sums[i] += val
            compare.pop(key, None)

        # set fuzzy name row
        rows[k] = (fuzzy_name, *fuzzy_sums) + rows[k][size:]

    def sort(row):
        name, count, *extras, score, key = row[size:]
        fuzzy_name, fuzzy_count, *fuzzy_sums = rows[key][0:size]
        break_tie_eq_fuzzy_values = fuzzy_count + 1 if fuzzy_name == name else count
        return (fuzzy_count, *fuzzy_sums, fuzzy_name,
                break_tie_eq_fuzzy_values, *extras, score, name)

    cols = tuple(df.columns.map(lambda col: f'Fuzzy{col}')) + \
        tuple(df.columns) + ('Similarity', 'key')

    return pd.DataFrame(sorted(rows, key=sort, reverse=True),
                        columns=cols).drop('key', axis=1)


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


def prepare_contacts(contacts_file, index, new=False):
    dfc = pd.read_csv(contacts_file, dtype={
        'RegistrationContactID': 'UInt32',
        'RegistrationID': 'UInt32'
    })
    if new:
        dfc = dfc[condo_coop_mask(dfc)]
    dfc = lower_case(dfc).drop_duplicates()
    # concat values for duplicate index rows
    dfc = dedup(dfc, index)
    return dfc.set_index(pd.Index(hash(dfc, index), name='hash'))


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
