# import settings
from pathlib import Path
import pandas as pd


def run(filename):
    iuu_list_orig = Path.cwd().parent.parent.joinpath('aux_data').joinpath(filename)
    pd_test = pd.read_csv(iuu_list_orig, delimiter="\t", encoding="ISO-8859-1")
    charges = ['IOTC', 'IOTC_Reason', 'ICCAT', 'ICCAT_Reason', 'IATTC', 'IATTC_Reason', 'WCPFC',
               'IATTC_Reason', 'CCAMLR', 'CCAMLR_Reason', 'SEAFO', 'SEAFO_Reason', 'INTERPOL',
               'INTERPOL_Reason', 'NEAFC', 'NEAFC_Reason', 'NAFO', 'NAFO_Reason',
               'SPRFMO', 'SPRFMO_Reason', 'CCSBT', 'CCSBT_Reason', 'GFCM', 'GFCM_Reason', 'NPFC',
               'NPFC_Reason', 'SIOFA', 'SIOFA_Reason']
    print(pd_test.columns)

    pd_test.rename(columns={'Reason': 'Reason_IOTC',
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

    print(pd_test.head(10))

    long = pd.wide_to_long(pd_test, stubnames=['Reason'], i=[], 'sep='_')



    #info_dict = {key: value for (key, value) in iuu.items() if key not in charges}
    #charges_dict = {key: value for (key, value) in iuu.items() if key in charges}
        #
        # info_df = pd.DataFrame(info_dict)
        # charges_df = pd.DataFrame(charges_dict)

        # print(charges_df.head())


if __name__ == '__main__':
    run('IUUList-20190902.txt')
