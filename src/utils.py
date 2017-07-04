import statistics


def mean(tuple_list):
    amounts = [tup[1] for tup in tuple_list]
    return statistics.mean(amounts)


def std_dev(tuple_list):
    amounts = [tup[1] for tup in tuple_list]
    return statistics.stdev(amounts)
