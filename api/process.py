from flask import request
from .mod import contacts, corporations
from .utils.diff import diff_frames
from .utils.fuzzy import fuzzyfy
from .utils.common import (
    export,
    read_csv,
    filename,
    parse_list,
    condo_coop_mask,
    ExportType,
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
    df = corporations.prepare(
        contacts_file,
        buildings_file,
        building_cols,
        filter_keywords)

    similarity = float(request.form.get('similarity') or 0)
    if similarity:
        ignore_keywords = parse_list(request.form.get('ignore-keywords'))
        df = fuzzyfy(df, similarity, ignore_keywords)

    file_name = filename(contacts_file, 'registration')
    return export(df, f'corporation-count-{file_name}-{similarity}')


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
        'LastName',
    ]

    contacts_old = contacts.prepare(contacts_old, index)
    contacts_new = contacts.prepare(contacts_new, index, new=True)

    old_rids = contacts_old['RegistrationID'].copy()

    dfc = diff_frames(
        contacts_old,
        contacts_new,
        ignore_cols=(*index,),
        show_atleast=(*index, 'BusinessZip'),
        removed_mask=condo_coop_mask)

    del contacts_old, contacts_new

    columns = ['BuildingID', 'Zip', 'RegistrationID'] + \
        parse_list(request.form.get('building-columns'))

    if not dfc.empty:
        buildings = read_csv(buildings, usecols=columns,
                             dtype={'BuildingID': 'UInt32',
                                    'RegistrationID': 'UInt32'})

        dfc = contacts.post_process(
            dfc, buildings, old_rids=old_rids,
            col_order={'first': ('ChangeType', *index),
                       'last': ('BusinessZip', 'Zip', 'ZipMatch')})

        del buildings, old_rids

    export_type = ExportType.EXCEL

    if export_type == ExportType.EXCEL:
        dfc = dfc.set_index('ChangeType')

    return export(dfc, f'compare-{old_name}-{new_name}', export_type)
