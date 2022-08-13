from flask import request, send_file
import pandas as pd
from .utils import filename, export, load_compare_df, diff

RPC = dict()


def register(fn):
    RPC[fn.__name__] = fn
    return fn


@register
def corporation_count():
    file = request.files.get('registration')
    df = pd.read_csv(file)
    df = df[['RegistrationID', 'CorporationName']].drop_duplicates()
    df = df.groupby(['CorporationName']).size().sort_values(
        ascending=False).reset_index(name='count')
    return export(df, f'corporation-count-{filename(file)}.csv')


@register
def compare():
    index = request.form.get('index-column')
    if not index:
        abort(400, 'Index column is required! 👉👈')

    old_file = request.files.get('old')
    new_file = request.files.get('new')

    old = load_compare_df(old_file, index)
    new = load_compare_df(new_file, index)

    df = diff(old, new, index)

    old_name = filename(old_file, 'old')
    new_name = filename(new_file, 'new')
    return export(df, f'compare-{old_name}-{new_name}.csv')
