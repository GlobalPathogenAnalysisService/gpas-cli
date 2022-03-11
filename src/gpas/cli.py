import defopt
import gpas

from gpas.validate import validate


def validate_cli(sample_sheet: str = '-'):
    '''
    Validate a given sample sheet
    :arg sample_sheet: Separator character
    '''
    if validate(sample_sheet):
        print('Validation passed')
    else:
        print('Validation failed')


def version_cli():
    '''
    Return gpas-cli version
    '''
    print(gpas.__version__)


def main():
    defopt.run({
        'validate': validate_cli,
        'version': version_cli
    })
