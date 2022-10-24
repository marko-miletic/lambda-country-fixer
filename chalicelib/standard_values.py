import pandas as pd

STANDARD_DATA_FRAME = pd.read_csv('./chalicelib/iso_countries.csv')

# standardized country code standard

def get_countries() -> set:
    """ return -> set() structure of standard countries """
    return set(STANDARD_DATA_FRAME['name'])

def get_codes_alpha3() -> set:
    """ return -> set() structure of standard alpha-3 codes """
    return set(STANDARD_DATA_FRAME['alpha-3'])

def get_codes_alpha2() -> set:
    """ return -> set() structure of standard alpha-2 codes """
    return set(STANDARD_DATA_FRAME['alpha-2'])

def get_country_code_combinations() -> dict:
    """ return -> dict(): dict['country'] = 'country code' """
    combinations = dict()
    for i in range(len(STANDARD_DATA_FRAME)):
        combinations[STANDARD_DATA_FRAME['name'][i]] = (
            STANDARD_DATA_FRAME['alpha-3'][i], STANDARD_DATA_FRAME['alpha-2'][i])
    return combinations