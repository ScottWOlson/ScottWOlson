import pandas as pd
from os import path
from io import BytesIO
from flask import send_file


def lower_case(df):
    def lower(col):
        if col.dtype == 'string':
            return col.str.lower()
        elif col.dtype == 'object':
            return col.map(
                lambda s:
                    s.lower() if isinstance(s, str) else s, na_action='ignore')
        return col
    return df.apply(lower)


def read_csv(file, default_dtype=None, dtype=None, lower_case=False, **kwargs):
    if default_dtype:
        if not isinstance(dtype, dict):
            raise Exception(
                'dtype must be a dict to parse remaining columns as default_dtype')
        header = pd.read_csv(file, nrows=0)
        file.seek(0)
        dtype = {
            **dict.fromkeys(
                header.columns.difference(dtype.keys()), default_dtype),
            **dtype,
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
    return send_file(bufferize(df, ext), mimetype[ext],
                     download_name=download_name,
                     as_attachment=True)


def hash_cols(df):
    df = df.astype('string').fillna('')
    cols = df.columns
    combined = df[cols[0]]
    for col in cols[1:]:
        combined += df[col]
    return combined


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
        lambda x: ' | '.join(
            x.dropna().astype(str).str.lower().drop_duplicates().sort_values()
        )).reset_index()
    return pd.concat([df[~duplicated], nodupes])


def condo_coop_mask(df):
    condo_coop = df['ContactDescription'].str.lower().isin([
        'condo', 'co-op'])
    return condo_coop


def not_contains_mask(series, keywords, **contains_args):
    if not keywords:
        return pd.Series(True, series.index)

    mask = True
    for word in keywords:
        mask &= ~series.str.contains(word, **contains_args)
    return mask
