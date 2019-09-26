# import settings
from pathlib import Path
import pandas as pd


def run(filename):
    iuu_list_orig = Path.cwd().parent.parent.joinpath('aux_data').joinpath(filename)
    with iuu_list_orig.open() as f:
        lines = f.readlines()
        for i, line in enumerate(lines):
            if i == 0:
                headers = [cell.strip() for cell in line.split(',')]
                headers = headers[0:19]
                charges = ['IOTC', 'IOTC_Reason', 'ICCAT', 'ICCAT_Reason', 'IATTC', 'IATTC_Reason', 'WCPFC',
                             'IATTC_Reason', 'CCAMLR', 'CCAMLR_Reason', 'SEAFO', 'SEAFO_Reason', 'INTERPOL',
                             'INTERPOL_Reason', 'NEAFC', 'NEAFC_Reason', 'NAFO', 'NAFO_Reason',
                             'SPRFMO', 'SPRFMO_Reason', 'CCSBT', 'CCSBT_Reason', 'GFCM', 'GFCM_Reason', 'NPFC',
                             'NPFC_Reason', 'SIOFA', 'SIOFA_Reason']
                headers.extend(charges)
                iuu = {cell: [] for cell in headers}
                print(iuu)
            else:
                line = [cell.strip() for cell in line.split(',')]
                for j, cell in enumerate(line):
                    try:
                        iuu[headers[j]].append(cell)
                    except IndexError:
                        print(j, ' ', cell)
    






if __name__ == '__main__':
    run('IUUList-20190902.csv')
