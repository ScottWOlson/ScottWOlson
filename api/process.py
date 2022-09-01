from flask import request
from .utils import (
    export,
    fuzzyfy,
    read_csv,
    filename,
    parse_list,
    diff_frames,
    condo_coop_mask,
    prepare_contacts,
    post_process_contacts,
    prepare_corporation_count,
)


RPC = dict()


def register(fn):
    RPC[fn.__name__] = fn
    return fn


@register
def corporation_count():
    contacts_file = request.files.get('registration')
    buildings_file = request.files.get('buildings')
    building_cols = parse_list(request.form.get('building-columns'))
    filter_keywords = parse_list(request.form.get('filter-keywords'))
    df = prepare_corporation_count(
        contacts_file,
        buildings_file,
        building_cols,
        filter_keywords)

    similarity = float(request.form.get('similarity') or 0)
    if similarity:
        df = fuzzyfy(
            df, similarity,
            parse_list(request.form.get('ignore-keywords')))
    return export(
        df, f'corporation-count-{filename(contacts_file, "registration")}-{similarity}.csv')


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
        # 'lower_case': True,
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

    del contacts_old
    del contacts_new

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
        
        del buildings
        del old_rids

    return export(dfc, f'compare-{old_name}-{new_name}.xlsx')
