import pandas as pd
from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype
import re
from functools import wraps
from pprint import pprint


class Data():
    def __init__(self, data):
        self.data = pd.read_csv(data, sep=',', error_bad_lines=False, index_col=False, dtype='unicode')

    @staticmethod
    def __getRanges(ranges):
        trange = set()
        for i in ranges:
            if type(i) == int:
                trange.add(i)
            elif '-' not in i:
                trange.add(int(i))
            else:
                lower_upper = list(map(int, i.split('-')))
                trange = trange | set(range(lower_upper[0], lower_upper[1] + 1))
        trange = list(map(str, trange))
        return trange

    @staticmethod
    def __printFunction(name, args, kwargs):
        args = list(map(lambda x: str(x), args))

        kwargs = list(map(lambda x: "{}={}".format(x, str(kwargs[x])), kwargs))

        arguments = args + kwargs
        arguments = ', '.join(arguments)

        return ("{}({})".format(name, arguments))


    def __printError(function):
        @wraps(function)
        def wrapper(*args, **kwargs):
            try:
                passed, error = function(*args, **kwargs)

                if not passed:
                    print("ERRORS".center(30, "*"))
                    print(Data.__printFunction(function.__name__, args, kwargs))
                    print("Errors appear in {} records".format(len(error)))

                    if kwargs.get('debug', False):
                        pprint(error)

                    print("*" * 30, end="\n\n")
            except Exception as e:
                print("ERRORS".center(30, "*"))
                print("Please double check your script :: {}".format(Data.__printFunction(function.__name__, args, kwargs)))
                print("Exception ::", e)
                print("*" * 30, end="\n\n")

        return wrapper


    @__printError
    def is_empty(self, qid, debug=False):
        '''
        return False if there is non-blank cell
        Otherwise True
        '''
        empty_row = self.data[qid].isnull()
        error = self.data[~empty_row]['record'].tolist()
        return not (len(error)), error

    @__printError
    def is_non_empty(self, qid, debug=False):
        '''
        return False if there is any blank cell.
        otherwise True
        '''
        empty_row = self.data[qid].isnull()
        error = self.data[empty_row]['record'].tolist()
        return not (len(error)), error

    @__printError
    def is_number(self, qid, blank=True, debug=False):
        '''
        return False i there is any value is not number
        Otherwise True
        '''

        number = self.data[qid].str.isdigit().fillna(blank)
        error = self.data[~number]['record'].tolist()

        return not (len(error)), error


    @__printError
    def is_float(self, qid, blank=True, debug=False):
        '''
        return False i there is any value is not number
        Otherwise True
        '''

        place_holder = blank or "NNN"

        number = self.data[qid].fillna(place_holder)

        result = []
        for i in number:
            try:
                i = float(i)
                result.append(True)
            except:
                result.append(False)

        error = self.data[~pd.Series(result)]['record'].tolist()

        return not (len(error)), error


    @__printError
    def check_range(self, qid, vrange, blank=True, debug=False):
        '''
        return False if any cells is out of a given range
        otherwise True
        blank = True, blank is allowed
        vrange = 1,'2','3' or '1-4'
        '''

        trange = Data.__getRanges(vrange)

        ranged = self.data[qid].isin(trange)
        empty_row = self.data[qid].isnull()

        if blank:
            error = self.data[~ranged & ~empty_row]['record'].tolist()
        else:
            error = self.data[~ranged]['record'].tolist()

        return not (len(error)), error


    @__printError
    def is_identical(self, qid1, qid2):
        '''
        return False if any cell is not matching
        otherwise True
        '''
        d_row = self.data[qid1] != self.data[qid2]
        error = self.data[d_row]['record'].tolist()
        return not (len(error)), error

    @staticmethod
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

    @classmethod
    def __generate_question(qid, rows, cols):
        '''
        Generate multi grid questions
        '''
        trows = Data.generate_rows_cols(rows)
        tcols = Data.generate_rows_cols(cols)

        print(trows)
        print(tcols)

        qids = []

    @__printError
    def check_checkbox(self, qid, options, exclusive=None, atleast=1, atmost=99999, blank=True, debug=False):
        toptions = set(Data.generate_rows_cols(options))
        texclusives = set(Data.generate_rows_cols(exclusive) if exclusive else [])
        toptions -= texclusives

        option_labels = []
        exclusive_labels = []

        for eachOption in toptions:
            option_labels.append('{}{}'.format(qid, eachOption))
        for eachOption in texclusives:
            exclusive_labels.append('{}{}'.format(qid, eachOption))

        tdata = self.data[option_labels + exclusive_labels]

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

        error = self.data[(atleast | atmost | exl) & nan ]['record'].tolist()

        return not (len(error)), error


    @__printError
    def check_logic(self, qid1, cond1, qid2, cond2, debug=False):
        '''
        check masking
        qid1: column name where cond based
        cond1: value to be checked in [] format
        qid2: column affected by the cond1
        cond2: accecpt able value in cond1
        '''

        trange1 = Data.__getRanges(cond1)
        data_cond1 = self.data[qid1].isin(list(map(str, trange1)))

        if cond2 is None:
            data_cond2 = self.data[qid2].isna()
        else:
            trange2 = Data.__getRanges(cond2)
            data_cond2 = self.data[qid2].isin(list(map(str, trange2)))

        error = self.data[data_cond1 & ~data_cond2]['record'].tolist()

        return not (len(error)), error

    @__printError
    def check_logic_checkbox(self, qid1, cond1, qid2, options, debug=False):
        '''
        :param qid1: qid where logic is based on
        :param cond1: masking logic
        :param qid2: checkbox question to be chcked
        :param options: options in the checkbox question
        '''

        options = Data.generate_rows_cols(options)
        option_labels = []
        for eachOption in options:
            option_labels.append('{}{}'.format(qid2, eachOption))

        tdata = self.data[option_labels]
        tdata = tdata.apply(pd.to_numeric)

        nan = pd.Series([True] * tdata.shape[0])
        for column in tdata:
            nan = nan & tdata[column].isna()

        data_cond1 = self.data[qid1].isin(map(str, cond1))

        error = self.data[data_cond1 & nan]['record'].tolist()

        return not (len(error)), error
