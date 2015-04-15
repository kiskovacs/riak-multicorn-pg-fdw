import operator
import re


class UnknownOperatorException(Exception):

    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return repr(self.msg)


def reverse_contains(a, b):
    return operator.contains(b, a)


def strictly_left(a, b):
    return max(a) < min(b)


def strictly_right(a, b):
    return min(a) > max(b)


def right_bounded(a, b):
    return max(a) <= max(b)


def left_bounded(a, b):
    return min(a) >= min(b)


def overlap(a, b):
    if (min(a) >= min(b)) and (min(a) <= max(b)):
        return True
    if (max(a) <= max(b)) and (max(a) >= min(b)):
        return True
    return False


def regex_search(a, b):
    if re.search(b, a):
        return True
    else:
        return False


def regex_search_i(a, b):
    if re.search(b, a, re.I):
        return True
    else:
        return False


def not_regex_search(a, b):
    return not regex_search(a, b)


def not_regex_search_i(a, b):
    return not regex_search_i(a, b)


def like_search(a, b):
    b.replace('%%', '.*')
    b.replace('_', '.')
    return regex_search(a, b)


def like_search_i(a, b):
    b.replace('%%', '.*')
    b.replace('_', '.')
    return regex_search_i(a, b)


def not_like_search(a, b):
    b.replace('%%', '.*')
    b.replace('_', '.')
    return not regex_search(a, b)


def not_like_search_i(a, b):
    b.replace('%%', '.*')
    b.replace('_', '.')
    return not regex_search_i(a, b)


################################################################################
# The main function we use external to this file:
# Translate a string with an operator in it (eg. ">=") into a function.
#
# Not supported (yet -- feel free to add more support!):
#    "between" -- it isn't clear if we'll get those.
#    "OR"      -- it isnt' clear if we'll get those.
#    Geometric Operators
#    Text Search Operators
#    Network Address Operators
#    JSON Operators
#    The Array operators when used on Ranges
def get_operator_function(opr):

    operator_function_map = {
        '<':          operator.lt,
        '>':          operator.gt,
        '<=':         operator.le,
        '>=':         operator.ge,
        '=':          operator.eq,
        '<>':         operator.ne,
        '!=':         operator.ne,
        '@>':         operator.contains,
        '<@':         reverse_contains,
        '<<':         strictly_left,
        '>>':         strictly_right,
        '&<':         right_bounded,
        '>&':         left_bounded,
        '&&':         overlap,
        'is':         operator.eq,  # this one won't work in every sql context, but should for some cases
        '~':          regex_search,
        '~*':         regex_search_i,
        '!~':         not_regex_search,
        '!~*':        not_regex_search_i,
        '~~':         like_search,
        '!~~':        not_like_search,
        'like':       like_search,
        'not like':   not_like_search,
        '~~*':        like_search_i,
        '!~~*':       not_like_search_i,
        'ilike':      like_search_i,
        'not ilike':  like_search_i,
        'similar to': regex_search,
        'not similar to': not_regex_search
    }

    if opr not in operator_function_map:
        raise UnknownOperatorException("'%s' is not a supported operator." % opr)

    return operator_function_map[opr]


