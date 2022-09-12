import re
import pandas as pd
from typing import Callable
from rapidfuzz import fuzz, process
from itertools import tee, chain, filterfalse
from typing import Optional


class IteratorWithItems:
    def __init__(self, iterator):
        self.iter = iterator

    def __next__(self):
        return next(self.iter)

    def __iter__(self):
        return self.iter

    def items(self):
        return self.iter


def split(predicate, iterator):
    i1, i2 = tee(iterator)
    return filter(predicate, i1), filterfalse(predicate, i2)


def sum_except(values, i):
    return sum(values[:i] + values[i + 1:])


def normalize(values):
    total = sum(values)
    return tuple(v / total for v in values)


def processor(ignore_keywords=None):
    EMPTY_STRING_GROUP = '#'
    ignore_regex = r'(?![\-.&])\W'
    if ignore_keywords:
        ignore_keywords = sorted(ignore_keywords, key=len, reverse=True)
        ignore_keywords = '|'.join(ignore_keywords).lower()
        ignore_regex = f'{ignore_keywords}|{ignore_regex}'
    ignore_regex = re.compile(ignore_regex)

    def process(s: str):
        processed = ignore_regex.sub(' ', s.lower())
        processed = ' '.join(sorted(processed.split()))
        return processed if processed else EMPTY_STRING_GROUP

    return process


def weighted_extract(query, choices, processor=None,
                     score_cutoff=0, short_circuit=False,
                     weights: Optional[tuple[float]] = None,
                     scorers: tuple[Callable] = (
                         fuzz.ratio, fuzz.token_set_ratio)):
    """
    Filter choices with averaged score based on specified scorers and weights.
    Refer to rapidfuzz.process.extract_iter for details.
    Placing slow scorers towards the end will maximize efficiency.

    Parameters
    ----------
    short_circuit   : whether to stop ratio computation
        as soon as score_cutoff threshold is reached.
    weights         : relative weights to compute average score of all scorers.
        defaults to equal weightage for each scorer.

    Returns
    -------
    Iterator of shortlisted choices as a tuple:
    ((key, weighted_score), choice)

    Examples
    --------
    matches = weighted_extract(
        'entropy calm down!',
        ['entropy come down', 'energy loves entropy'],
        processor=None,
        score_cutoff=90,
        short_circuit=False,
        scorers=(fuzz.token_sort_ratio, fuzz.token_set_ratio),
        weights=(3, 7))

    """
    if weights is None:
        weights = map(lambda _: 1, scorers)
    weights = normalize(weights)

    shorted = []
    choices_iter = choices.items() if hasattr(choices, 'items') \
        else enumerate(choices)
    matches = IteratorWithItems(
        map(lambda c: ((c[0], 0), c[1]), choices_iter))
    for i, scorer in enumerate(scorers):
        # TODO:
        # generalize for rapidfuzz.string_metric functions
        #   that return score between 0 to 1.
        # verify whether ε subtraction due to floating point error
        #   will be necessary
        min_cutoff = (
            score_cutoff - (100 * sum_except(weights, i))
        ) / weights[i]
        if min_cutoff < 0:
            min_cutoff = 0

        matches = process.extract_iter(
            query,
            matches,
            score_cutoff=min_cutoff,
            processor=processor,
            scorer=scorer)

        def reduce(match, weight=weights[i]):
            sub_query, score, (key, old_score) = match
            score = old_score + (score * weight)
            return ((key, score), sub_query)

        max_remaining_score = 100 * sum(weights[i + 1:])

        def reachable(match, max_remaining_score=max_remaining_score):
            (_, score), _ = match
            return (score + max_remaining_score) >= score_cutoff

        matches = map(reduce, matches)
        matches = filter(reachable, matches)

        if short_circuit:
            shorts, matches = split(
                lambda m: m[0][1] >= score_cutoff, matches)
            shorted.append(shorts)

        matches = IteratorWithItems(matches)

    matches = filter(lambda m: m[0][1] >= score_cutoff, matches)
    return chain(*shorted, matches)


def fuzzyfy(df: pd.DataFrame, similarity: float = 90,
            ignore_keywords: Optional[list] = None):
    """
    Groupby fuzzyfied names and aggregate Count and optional summable columns:
    - names are lowercased,
        stripped of whitespace, `ignore_keywords`, and most alphanumeric chars,
        token sorted by words
    - grouped together based on `similarity`.
    - finally, their Count and *ExtraColumns
        are aggregated as FuzzyCount and *FuzzySumExtraColumns.

    Complexity:
        Each iteration removes matched names from comparison.
        Let k be the maximum group size for a given similarity,
        and n be the number of names.
        If every group is of size k, we arrive at a lower-bound
        of O(n²/2k). Any other distribution of group sizes performs worse
        with left skewed ones approaching O(n²).

    Existence of a better than quadratic time algorithm:
        Consider sorting as an example: If you know b is less than
        remainder of the list, and a < b, you're wasting cycles comparing
        a with remainder of the list. Hence why, sorting can be optimally
        performed in O(n·log(n)). Similar principle applies here,
        if you can conclusively prove, that checking a particular subset of discarded
        ratios from previous iterations leads to a worse similarity score,
        the time complexity may be substantially improved.
        Indel/Levenshtein edit distancing is a lossy operation by nature
        but, perhaps an upper bound exists to help filter similar ratios
        for successive iterations. With the right ordering of names,
        initial-size-dependent decay will result in a different function class altogether!
        Specifically, for a given distribution, we generate the maximally decaying curve Ψ(i, n),
        where Ψ is the comparison input size at ith iteration starting with n names.
        Then, the time complexity can be expressed as Ω(integral(0, n, Ψ(i,n)·di)).

    Parameters
    ----------
    df               : dataframe with Name: string, Count: number, *ExtraColumns: number columns.
    similarity       : number between 0-100; names to be considered within the same group,
        if they are `similarity` percent match.
    ignore_keywords  : keywords to be ignored from comparison.

    Returns
    -------
    dataframe with FuzzyName, FuzzyCount, *FuzzySumExtraColumns,
                   Name, Count, *ExtraColumns, Similarity columns.

    """
    size = len(df.columns)
    pd_nas = (pd.NA,) * size

    TOKEN_SET_WEIGHT = 0.725
    weights = (1 - TOKEN_SET_WEIGHT, TOKEN_SET_WEIGHT)
    scorers = (fuzz.ratio, fuzz.token_set_ratio)

    name_col = df.columns[0]
    # we got memory but no time! 🏃
    compare = dict(df[name_col].apply(processor(ignore_keywords)))
    rows = list(df.values)
    for k, row in enumerate(rows):
        # ignore if already processed
        if len(row) > size:
            continue

        matches = weighted_extract(
            compare[k],
            compare,
            processor=None,
            scorers=scorers,
            weights=weights,
            short_circuit=False,
            score_cutoff=similarity)

        # Steering away from dataframe indexing - required when groupby.agg
        # was used - provided considerable performance benefit in the past.
        # Now with rapidfuzz, we may switch back to pandas aggregation
        # to improve readability. But, customized sorting
        # is still best suited on df.values.
        fuzzy_name = row[0]
        fuzzy_sums = [0] * (size - 1)
        for (key, score), _ in tuple(matches):
            _, *values = rows[key]
            rows[key] = pd_nas + (*rows[key], score, k)
            for i, val in enumerate(values):
                fuzzy_sums[i] += val
            compare.pop(key)

        # set fuzzy name row
        rows[k] = (fuzzy_name, *fuzzy_sums) + rows[k][size:]

    def sort(row):
        name, count, *extras, score, key = row[size:]
        fuzzy_name, fuzzy_count, *fuzzy_sums = rows[key][0:size]
        break_tie_eq_fuzzy_values = fuzzy_count + 1 \
            if fuzzy_name == name else count
        return (fuzzy_count, *fuzzy_sums, fuzzy_name,
                break_tie_eq_fuzzy_values, *extras, score, name)

    columns = tuple(df.columns.map(lambda col: f'Fuzzy{col}')) + \
        tuple(df.columns) + ('Similarity', 'key')

    return pd.DataFrame(sorted(rows, key=sort, reverse=True),
                        columns=columns).drop('key', axis=1)
