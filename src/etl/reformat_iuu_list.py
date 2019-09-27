# import settings
from pathlib import Path
import pandas as pd
from utils import copy_csv_to_db


def run(filename):
    iuu_list_orig = Path.cwd().parent.parent.joinpath('aux_data').joinpath(filename)
    pd_test = pd.read_csv(iuu_list_orig, delimiter="\t", encoding="ISO-8859-1")
    charges = ['IOTC', 'IOTC_Reason', 'ICCAT', 'ICCAT_Reason', 'IATTC', 'IATTC_Reason', 'WCPFC',
               'IATTC_Reason', 'CCAMLR', 'CCAMLR_Reason', 'SEAFO', 'SEAFO_Reason', 'INTERPOL',
               'INTERPOL_Reason', 'NEAFC', 'NEAFC_Reason', 'NAFO', 'NAFO_Reason',
               'SPRFMO', 'SPRFMO_Reason', 'CCSBT', 'CCSBT_Reason', 'GFCM', 'GFCM_Reason', 'NPFC',
               'NPFC_Reason', 'SIOFA', 'SIOFA_Reason']

    pd_test.rename(columns={'IOTC': 'Date_IOTC',
                            'ICCAT': 'Date_ICCAT',
                            'IATTC': 'Date_IATTC',
                            'WCPFC': 'Date_WCPFC',
                            'CCAMLR': 'Date_CCAMLR',
                            'SEAFO': 'Date_SEAFO',
                            'INTERPOL': 'Date_INTERPOL',
                            'NEAFC': 'Date_NEAFC',
                            'NAFO': 'Date_NAFO',
                            'SPRFMO': 'Date_SPRFMO',
                            'CCSBT': 'Date_CCSBT',
                            'GFCM': 'Date_GFCM',
                            'NPFC': 'Date_NPFC',
                            'Reason': 'Reason_IOTC',
                            'Reason.1': 'Reason_ICCAT',
                            'Reason.2': 'Reason_IATTC',
                            'Reason.3': 'Reason_WCPFC',
                            'Reason.4': 'Reason_CCAMLR',
                            'Reason.5': 'Reason_SEAFO',
                            'Reason.6': 'Reason_INTERPOL',
                            'Reason.7': 'Reason_NEAFC',
                            'Reason.8': 'Reason_NAFO',
                            'Reason.9': 'Reason_SPRFMO',
                            'Reason.10': 'Reason_CCSBT',
                            'Reason.11': 'Reason_GFCM',
                            'Reason.12': 'Reason_NPFC',
                            'Reason.13': 'Reason_SIOFA',
                            }, inplace=True)

    pd_test = pd_test.reset_index()
    long = pd.wide_to_long(pd_test, i='index', j='Violation', stubnames=['Reason', 'Date'], sep="_", suffix='\\w+') \
             .reset_index()

    long = long.dropna(how = 'all', subset=['Reason', 'Date'])

    print(long.shape)


if __name__ == '__main__':
    run('IUUList-20190902.txt')
