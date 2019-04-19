import pandas as pd
from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype
import re
from functools import wraps
from pprint import pprint

data = pd.read_csv('../data/data.csv',sep=',', error_bad_lines=False, index_col=False, dtype='unicode')


def printFunction(name, args, kwargs):
    args = list(map(lambda x: str(x), args))

    kwargs = list(map(lambda x: "{}={}".format(x, str(kwargs[x])), kwargs))

    arguments = args + kwargs
    arguments = ', '.join(arguments)

    return ("{}({})".format(name, arguments))


def printError(function):
    @wraps(function)
    def wrapper(*args, **kwargs):
        try:
            passed, error = function(*args, **kwargs)

            if not passed:
                print("ERRORS".center(30, "*"))
                print(printFunction(function.__name__, args, kwargs))
                print("Errors appear in {} records".format(len(error)))

                if kwargs.get('debug', False):
                    pprint(error)

                print("*" * 30, end="\n\n")
        except Exception as e:
            print("ERRORS".center(30, "*"))
            print("Please double check your script :: {}".format(printFunction(function.__name__, args, kwargs)))
            print("Exception ::", e)
            print("*" * 30, end="\n\n")

    return wrapper


@printError
def is_empty(qid, debug=False):
    '''
    return False if there is non-blank cell
    Otherwise True
    '''
    empty_row = data[qid].isnull()
    error = data[~empty_row]['record'].tolist()
    return not (len(error)), error


@printError
def is_non_empty(qid, debug=False):
    '''
    return False if there is any blank cell.
    otherwise True
    '''
    empty_row = data[qid].isnull()
    error = data[empty_row]['record'].tolist()
    return not (len(error)), error


@printError
def is_number(qid, debug=False):
    '''
    return False i there is any value is not number
    Otherwise True
    '''

    number = data[qid].str.isdigit().fillna(True)
    error = data[~number]['record'].tolist()

    return not (len(error)), error


@printError
def is_float(qid, debug=False):
    '''
    return False i there is any value is not number
    Otherwise True
    '''
    number = data[qid]

    result = []
    for i in number:
        try:
            i = float(i)
            result.append(True)
        except:
            result.append(False)

    error = data[~pd.Series(result)]['record'].tolist()

    return not (len(error)), error


@printError
def check_range(qid, vrange, blank=True, debug=False):
    '''
    return False if any cells is out of a given range
    otherwise True
    blank = True, blank is allowed
    vrange = 1,'2','3' or '1-4'
    '''
    trange = set()
    for i in vrange:
        if type(i) == int:
            trange.add(i)
        elif '-' not in i:
            trange.add(int(i))
        else:
            lower_upper = list(map(int, i.split('-')))
            trange = trange | set(range(lower_upper[0], lower_upper[1] + 1))

    trange = list(map(str, trange))

    ranged = data[qid].isin(trange)
    empty_row = data[qid].isnull()

    if blank:
        error = data[~ranged & ~empty_row]['record'].tolist()
    else:
        error = data[~ranged]['record'].tolist()

    return not (len(error)), error


@printError
def is_identical(qid1, qid2):
    '''
    return False if any cell is not matching
    otherwise True
    '''
    d_row = data[qid1] != data[qid2]
    error = data[d_row]['record'].tolist()
    return not (len(error)), error


def generate_rows_cols(labels):
    '''
    generate row/col labels
    li : ['r:1-4']
    '''
    output = set()
    for label in labels:
        if ':' in label:
            label_range = label.split(':')
            lower_upper = label_range[1].split('-')
            for i in range(int(lower_upper[0]), int(lower_upper[1]) + 1):
                output.add('{}{}'.format(label_range[0], i))
        else:
            output.add(label)

    output = list(output)

    # find a label
    regex = r'[a-zA-Z]*'
    label = re.match(regex, output[0])
    length = len(label.group())

    return sorted(output, key=lambda x: int(x[length:]))


def generate_question(qid, rows, cols):
    '''
    Generate multi grid questions
    '''
    trows = generate_rows_cols(rows)
    tcols = generate_rows_cols(cols)

    print(trows)
    print(tcols)

    qids = []


#     for row in trows:
#         for col in tcols:
#             qids.append('{}{}{}'.format(qid,row,col))

#     return qids

@printError
def check_checkbox(qid, options, exclusive=None, atleast=1, atmost=99999, blank=True, debug=False):
    toptions = set(generate_rows_cols(options))
    texclusives = set(generate_rows_cols(exclusive) if exclusive else [])
    toptions -= texclusives

    option_labels = []
    exclusive_labels = []

    for eachOption in toptions:
        option_labels.append('{}{}'.format(qid, eachOption))
    for eachOption in texclusives:
        exclusive_labels.append('{}{}'.format(qid, eachOption))

    tdata = data[option_labels + exclusive_labels]

    nan = pd.Series([True] * tdata.shape[0])
    if blank:
        for column in tdata:
            nan = nan & tdata[column].isna()
        nan = ~nan

    tdata = tdata.apply(pd.to_numeric)

    tdata = tdata.assign(total=tdata.sum(axis=1, skipna=True))
    tdata = tdata.assign(osum=tdata[option_labels].sum(axis=1, skipna=True))
    tdata = tdata.assign(esum=tdata[exclusive_labels].sum(axis=1, skipna=True))

    atleast = tdata['total'] < atleast
    atmost = tdata['total'] > atmost
    exl = (tdata['osum'] > 0) & (tdata['esum'] == 1)

    error = data[(atleast | atmost | exl) & nan ]['record'].tolist()

    return not (len(error)), error


@printError
def check_logic(qid1, cond1, qid2, cond2):
    '''
    check masking
    qid1: column name where cond based
    cond1: value to be checked in [] format
    qid2: column affected by the cond1
    cond2: accecpt able value in cond1
    '''

    data_cond1 = data[qid1].isin(cond1)
    data_cond2 = data[qid2].isin(cond2)

    error = data[data_cond1 & ~data_cond2]['record'].tolist()

    return not (len(error)), error


def check_checkbox_multi_grid(qid, rows, cols, atleast=1, atmost=9999, grouping='rows'):
    trows = generate_rows_cols(rows)
    tcols = generate_rows_cols(cols)

    if grouping == 'rows':
        for eachRow in trows:
            group = []
            for eachCol in tcols:
                group.append(('{}{}{}').format(qid, eachRow, eachCol))
            print(group)
    else:
        for eachCol in tcols:
            group = []
            for eachRow in trows:
                group.append(('{}{}{}').format(qid, eachRow, eachCol))
            print(group)


# is_non_empty('S5')
# is_identical('S1', 'S2')
# check_range('S1',['1-3'], blank=False)
# check_checkbox('S4', ['r:1-5', 'r:7-10', 'r98'])
# check_logic('hidPanel', [2], 'S5', [1,2,3])
# check_logic('hidPanel', [1], 'S5', [None])

#######SCRIPT########
# is_non_empty('record')
# is_number('record')

# check_range('dTrack', [0], blank=False)
# check_range('status', [3], blank=False)

# is_number('dmaDataZIP_CODEc2')

#     tdata = tdata.apply(pd.to_numeric)

#     #     tadata = tdata.loc[:,'total'] = tdata.sum(axis = 1, skipna = True)

#     tdata = tdata.assign(total=tdata.sum(axis=1, skipna=True))

#     atleast = tdata['total'] < atleast
#     atmost = tdata['total'] > atmost

#     error = data[atleast | atmost]['record'].tolist()

#     return not (len(error)), error

check_checkbox('HIC01', ['r:1-6', 'r:98-99'], ['r98', 'r99'], blank=False)

check_checkbox('HIC02', ['r:1-4', 'r98'], ['r98'])

check_checkbox('progHIC03', ['r:1-4', 'r97'], debug=True)