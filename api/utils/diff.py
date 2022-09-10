import pandas as pd
from typing import Callable


def index_changes(odf, ndf):
    index_name = ndf.index.name or 'index'
    changes = pd.DataFrame(
        ndf.index,
        columns=[index_name]
    ).merge(
        pd.DataFrame(
            odf.index,
            columns=[index_name]),
        on=index_name,
        how='outer',
        indicator=True)

    changes = changes.rename(columns={'_merge': 'ChangeType'})
    changes['ChangeType'] = changes['ChangeType'].map(
        {'left_only': 'added', 'both': 'changed', 'right_only': 'removed'})

    return changes.set_index(index_name)


def diff_frames(odf: pd.DataFrame, ndf: pd.DataFrame,
                ignore_cols: tuple[str] = (),
                show_atleast: tuple[str] = (),
                removed_mask: Callable[[pd.DataFrame], pd.Series] = None):
    """
    Compare dataframes with identical columns on index

    Parameters
    ----------
    odf          : old dataframe
    ndf          : new dataframe
    ignore_cols  : columns to ignore from comparison
    show_atleast : show atleast these columns even when no change
    removed_mask : accepts removed rows as dataframe and
        retuns identically indexed boolean mask for rows to keep

    """
    cdf = index_changes(odf, ndf)

    removed = cdf['ChangeType'] == 'removed'
    if removed_mask:
        removed = removed_mask(odf[removed])
        removed = pd.Series(removed, odf.index).fillna(False)
    removed = odf[removed]

    added = ndf[cdf['ChangeType'] == 'added']

    changed = cdf['ChangeType'] == 'changed'
    changed = changed.index[changed]
    subset = ndf.columns.difference(ignore_cols)
    changed = odf.loc[changed, subset].compare(
        ndf.loc[changed, subset]).convert_dtypes()

    changed.columns = changed.columns.set_levels(['new', 'old'], level=1)

    changed_cols = changed.columns.levels[0]

    for col in show_atleast:
        if col not in changed_cols:
            changed[col] = ndf.loc[changed.index, col]

    # convert to multiindex for concatenation
    added.columns = added.columns.map(
        lambda c: (c, 'new') if c in changed_cols else (c, ''))
    removed.columns = removed.columns.map(
        lambda c: (c, 'old') if c in changed_cols else (c, ''))

    combined = pd.concat([changed, added, removed]).dropna(how='all', axis=1)
    combined.columns = combined.columns.remove_unused_levels()

    combined['ChangeType'] = cdf.loc[combined.index, 'ChangeType']

    return combined
