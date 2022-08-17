import pandas as pd
from flask import request, abort
from .utils import filename, export, diff, parse_contacts


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
def compare_registration():
    buildings_old = request.files.get('buildings-old')
    buildings_new = request.files.get('buildings-new')
    contacts_old = request.files.get('contacts-old')
    contacts_new = request.files.get('contacts-new')

    old_name = filename(contacts_old, 'old')
    new_name = filename(contacts_new, 'new')

    contacts_old = parse_contacts(contacts_old, buildings_old)
    contacts_new = parse_contacts(contacts_new, buildings_new)

    df = diff(contacts_old, contacts_new, 'hash', [
              'RegistrationContactID', 'RegistrationID'])

    return export(df, f'compare-{old_name}-{new_name}.csv')


@register
def compare():
    index = request.form.get('index-column')
    if not index:
        abort(400, 'Index column is required! 👉👈')

    old_file = request.files.get('old')
    new_file = request.files.get('new')

    old_name = filename(old_file, 'old')
    new_name = filename(new_file, 'new')

    df = diff(old_file, new_file, index)

    return export(df, f'compare-{old_name}-{new_name}.csv')
