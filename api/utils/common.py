import pandas as pd
from os import path
from enum import Enum
from io import BytesIO
from flask import send_file


class ExportType(Enum):
    EXCEL = 'xlsx'
    CSV = 'csv'

    def __str__(self):
        return self.value


def lowercase(df):
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
        df = lowercase(df)

    return df


def parse_list(string):
    return string.split(',') if string else []


def filename(file, default=''):
    return path.splitext(path.basename(
        getattr(file, 'filename') or default))[0]


def bufferize(df, export_type: ExportType = ExportType.CSV):
    buffer = BytesIO()
    if export_type == ExportType.EXCEL:
        df.to_excel(buffer, encoding='utf-8')
    else:
        df.to_csv(buffer, index=False, encoding='utf-8')
    buffer.seek(0)
    return buffer


mimetype = {
    ExportType.CSV: 'text/csv',
    ExportType.EXCEL: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
}


def export(df: pd.DataFrame, filename: str,
           export_type: ExportType = ExportType.CSV):
    return send_file(bufferize(df, export_type), mimetype[export_type],
                     download_name=f'{filename}.{export_type}',
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
