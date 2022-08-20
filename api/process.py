import pandas as pd
from flask import request, abort
from .utils import (
    diff,
    export,
    filename,
    diff_frames,
    condo_coop_mask,
    prepare_contacts,
    post_process_contacts)


RPC = dict()


def register(fn):
    RPC[fn.__name__] = fn
    return fn


@register
def corporation_count():
    file = request.files.get('registration')
    df = pd.read_csv(file)
    df = df[condo_coop_mask(df)]
    df = df[['RegistrationID', 'CorporationName']].drop_duplicates()
    df = df.groupby(['CorporationName']).size().sort_values(
        ascending=False).reset_index(name='count')
    return export(
        df, f'corporation-count-{filename(file, "registration")}.csv')


@register
def compare_contacts():
    buildings = request.files.get('buildings')
    contacts_old = request.files.get('contacts-old')
    contacts_new = request.files.get('contacts-new')

    old_name = filename(contacts_old, 'old')
    new_name = filename(contacts_new, 'new')

    index = [
        'RegistrationContactID',
        'RegistrationID',
        'FirstName',
        'MiddleInitial',
        'LastName']

    contacts_dtypes = {'BusinessZip': 'string'}
    contacts_old = prepare_contacts(
        pd.read_csv(contacts_old, dtype=contacts_dtypes), index, old=True)
    contacts_new = prepare_contacts(
        pd.read_csv(contacts_new, dtype=contacts_dtypes), index)

    dfc = diff_frames(
        contacts_old,
        contacts_new,
        ignore_cols=index,
        show_atleast=[*index, 'BusinessZip']
        removed_mask=condo_coop_mask)

    if not dfc.empty:
        buildings = pd.read_csv(
            buildings,
            usecols=[
                'BuildingID',
                'RegistrationID',
                'LowHouseNumber',
                'HighHouseNumber',
                'StreetName',
                'Zip'],
            dtype={'BuildingID': 'Int64', 'Zip': 'string'})

        dfc = post_process_contacts(
            dfc,
            buildings,
            old_rids=contacts_old['RegistrationID'],
            col_order={'first': ['ChangeType', *index], 'last': ['BusinessZip', 'Zip', 'ZipMatch']})

    return export(dfc, f'compare-{old_name}-{new_name}.csv')


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
