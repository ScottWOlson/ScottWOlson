import pandas as pd
from flask import request, abort
from .utils import (
    diff,
    export,
    read_csv,
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
    dtype = {
        'RegistrationID': 'Int64',
        'CorporationName': 'string',
        'ContactDescription': 'string'}
    df = read_csv(file, usecols=list(dtype.keys()), dtype=dtype)
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

    read_contacts_args = {
        # 'default_dtype': 'string',
        'dtype': {
            'RegistrationContactID': 'Int64',
            'RegistrationID': 'Int64'},
        # 'lower_case': True
    }
    contacts_old = prepare_contacts(
        read_csv(
            contacts_old,
            **read_contacts_args),
        index,
        old=True)
    contacts_new = prepare_contacts(
        read_csv(
            contacts_new,
            **read_contacts_args),
        index)

    # copy not necessary at this point but
    # in case diff alters contacts df in future
    old_rids = contacts_old['RegistrationID'].copy()

    dfc = diff_frames(
        contacts_old,
        contacts_new,
        ignore_cols=index,
        show_atleast=[*index, 'BusinessZip'],
        removed_mask=condo_coop_mask)

    if not dfc.empty:
        buildings = read_csv(
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
            old_rids=old_rids,
            col_order={'first': ['ChangeType', *index], 'last': ['BusinessZip', 'Zip', 'ZipMatch']})

    return export(dfc, f'compare-{old_name}-{new_name}.xlsx')


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
