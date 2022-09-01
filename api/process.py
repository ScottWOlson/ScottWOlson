from flask import request, abort
from .utils import (
    diff,
    export,
    fuzzyfy,
    read_csv,
    filename,
    parse_list,
    diff_frames,
    condo_coop_mask,
    not_contains_mask,
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
    df = read_csv(
        file,
        usecols=dtype.keys(),
        dtype=dtype).dropna(
        subset='CorporationName')

    # extract relevant subset
    df = df[condo_coop_mask(df)]
    df = df[not_contains_mask(df['CorporationName'],
                              parse_list(request.form.get('filter-keywords')),
                              regex=False, case=False)]
    df = df[['RegistrationID', 'CorporationName']].drop_duplicates()

    # sorting has added benefit of optimizing fuzzyfy:
    # assuming names that repeat most often also contain the largest
    # number of spelling errors.
    df = df.groupby(['CorporationName']).size().sort_values(
        ascending=False).reset_index(name='Count')

    similarity = float(request.form.get('similarity') or 0)
    if similarity:
        df = fuzzyfy(
            df, similarity,
            parse_list(request.form.get('ignore-keywords')))
    return export(
        df, f'corporation-count-{filename(file, "registration")}-{similarity}.csv')


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
        'LastName'
    ]

    read_contacts_args = {
        # 'lower_case': True
        # 'default_dtype': 'string',
        'dtype': {
            'RegistrationContactID': 'Int64',
            'RegistrationID': 'Int64'
        },
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

    cols = ['BuildingID', 'Zip', 'RegistrationID'] + \
        parse_list(request.form.get('building-columns'))

    if not dfc.empty:
        buildings = read_csv(
            buildings,
            usecols=cols,
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
