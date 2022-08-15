from flask import request, abort
import pandas as pd
from .utils import filename, export, diff, parse_registration

RPC = dict()


def register(fn):
    RPC[fn.__name__] = fn
    return fn


@register
def corporation_count():
    file = request.files.get('registration')
    df = pd.read_csv(file)
    df = df[df['ContactDescription'].str.upper().isin(['CO-OP', 'CONDO'])]
    df = df[['RegistrationID', 'CorporationName']].drop_duplicates()
    df = df.groupby(['CorporationName']).size().sort_values(
        ascending=False).reset_index(name='count')
    return export(
        df, f'corporation-count-{filename(file, "registration")}.csv')


@register
def compare():
    index = request.form.get('index-column')
    if not index:
        abort(400, 'Index column is required! 👉👈')

    old_file = request.files.get('old')
    new_file = request.files.get('new')

    old_name = filename(old_file, 'old')
    new_name = filename(new_file, 'new')

    if index == 'RegistrationContactID':
        old_file = parse_registration(old_file)
        new_file = parse_registration(new_file)

    df = diff(old_file, new_file, index)

    return export(df, f'compare-{old_name}-{new_name}.csv')
