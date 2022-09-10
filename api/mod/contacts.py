import pandas as pd
from ..utils.common import (
    dedup,
    hash_cols,
    lower_case,
    condo_coop_mask,
)


def prepare(contacts_file, index, new=False):
    dfc = pd.read_csv(contacts_file, dtype={
        'RegistrationContactID': 'UInt32',
        'RegistrationID': 'UInt32'
    })
    if new:
        dfc = dfc[condo_coop_mask(dfc)]
    dfc = lower_case(dfc).drop_duplicates()
    # concat values for duplicate index rows
    dfc = dedup(dfc, index)
    index_col = pd.Index(hash_cols(dfc[index]), name='hash')
    return dfc.set_index(index_col)


def post_process(contacts, buildings, old_rids,
                          col_order: dict = None):
    dfc = contacts

    # detect new-buildings
    added = dfc['ChangeType'] == 'added'
    new_rids = ~dfc.loc[added, 'RegistrationID'].isin(old_rids)
    dfc['ChangeType'] = dfc['ChangeType'].cat.add_categories('new-building')
    dfc.loc[
        pd.Series(new_rids, dfc.index).fillna(False),
        'ChangeType'] = 'new-building'

    # merge buildings info
    buildings.columns = buildings.columns.map(lambda c: (c, ''))
    dfc = dfc.merge(buildings, on='RegistrationID', how='left')

    # zip match
    biz_zip_col = ('BusinessZip', 'new')
    if biz_zip_col not in dfc.columns:
        biz_zip_col = 'BusinessZip'
    dfc['Zip'] = dfc['Zip'].convert_dtypes()
    dfc[biz_zip_col] = dfc[biz_zip_col].convert_dtypes()
    dfc.loc[dfc[biz_zip_col] == dfc['Zip'], 'ZipMatch'] = 'Y'

    # rearrange columns
    if col_order is not None:
        first = col_order.get('first') or ()
        last = col_order.get('last') or ()
        columns = first + \
            (*dfc.columns.levels[0].difference(first + last),) + last
        dfc = dfc.reindex(columns=columns, level=0, copy=False)

    return dfc.set_index('ChangeType')
