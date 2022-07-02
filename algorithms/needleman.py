from . import df, alpha, miss_cost
from collections import Counter


import numpy as np


def create_matrix(seq_a, seq_b):
    matrix = []
    for a in seq_a:
        new_row = []
        for b in seq_b:
            new_row.append(df[a][b])
        matrix.append(new_row)
    return matrix

# coords: (score, way)

cache_dict = {}


def get_max_way(matrix, seq_a, seq_b, i, j):
    if (i, j) in cache_dict:
        return cache_dict[(i, j)]
    if i == -1 or j == -1:
        cache_dict[(i, j)] = (i + j + 2) * miss_cost, (i + j + 2) * ['_']
        return cache_dict[(i, j)]
    if seq_a[i] == seq_b[j]:
        prev_cost, prev_way = get_max_way(matrix, seq_a, seq_b, i - 1, j - 1) # df[seq_a[i]][seq_b[j]]
        cache_dict[(i, j)] = prev_cost + df[seq_a[i]][seq_b[j]], prev_way + [seq_a[i]]
        return cache_dict[(i, j)]
    elif seq_a[i] != seq_b[j]:
        prev_cost_1, prev_way_1 = get_max_way(matrix, seq_a, seq_b, i - 1, j - 1)  # df[seq_a[i]][seq_b[j]]
        prev_cost_2, prev_way_2 = get_max_way(matrix, seq_a, seq_b, i - 1, j)  # miss_cost
        curr_cost_1, curr_cost_2 = prev_cost_1 + df[seq_a[i]][seq_b[j]], prev_cost_2 + miss_cost
        if curr_cost_1 > curr_cost_2:
            cache_dict[(i, j)] = curr_cost_1 + df[seq_a[i]][seq_b[j]], prev_way_1 + ['!']
        else:
            cache_dict[(i, j)] = curr_cost_2 + miss_cost, prev_way_2 + ['_']
        return cache_dict[(i, j)]


def needleman_func(seq_a, seq_b):
    matrix = create_matrix(seq_a, seq_b)
    return get_max_way(matrix, seq_a, seq_b, len(seq_a) - 1, len(seq_b) - 1)


def result_analytics(changed_seq):
    words = changed_seq.split('_')
    words = [word for word in words if word]
    lags_num = len(changed_seq) - sum((len(word) for word in words))
    median_word_len = len(sorted(words, key=len)[len(words) // 2])
    middle_lag = lags_num / (len(words) - 1) if len(words) > 1 else lags_num
    correct_words_percent = sum([1 - Counter(word)['!'] / len(word) for word in words]) / len(words)
    return median_word_len, middle_lag, correct_words_percent


