from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import time
import pandas as pd


def boat_text_cleaner(boat_text_list):
    """
    Cleans text incoming from Selenium boat db scrape.
    Parameters:
    boat_text_list: list
        List containing text from boat database Selenium scrape
    Returns:
    row: list
        Records to be uploaded to boat entity table in Postgres
    """

    selected_columns = ['MMSI', 'IMO', 'Call Sign', 'Name', 'Flag',
                        'Home port', 'Year Built', 'AIS Vessel Type',
                        'Vessel Type', 'Gross Tonnage', 'Deadweight',
                        'Length', 'Breadth', 'Photo']
    boat_dict = {}
    row = []

    for cell in boat_text_list:

        # Split cell into field and element
        cell_components = cell.split(':')
        field = cell_components[0]
        element = cell_components[1].strip()

        # Check if this field already exists
        if field in boat_dict:
            continue

        # Check for invalid cell entry
        if element in {'-', '', 'N/a'}:
            element = 'NULL'

        if field == 'Name':

            # Colon in ship name
            if len(cell_components) > 2 and element != 'NULL':

                # Create list to append name pieces
                name_list = []

                # Split with space for renaming
                name_components = cell.split(': ')

                # Loop through pieces, correct, and append to list
                for string_piece in name_components[1:-1]:
                    corrected_colon = string_piece + ': '
                    name_list.append(corrected_colon)

                # Append final piece and join list together
                name_list.append(name_components[-1])
                element = ''.join(name_list)

        elif field == 'Flag':

            if element == 'NULL':
                pass
            else:
                element = element.split(' [')[0]

        elif field == 'Deadweight':

            if element == 'NULL':
                pass
            else:
                element = element[:-2]

        elif field == 'Length Overall x Breadth Extreme':

            # Check for invalid entry in special 1->2 column
            if element == 'NULL':
                boat_dict['Length'] = element
                boat_dict['Breadth'] = element
                continue

            else:
                ship_dimensions = element.split('m × ')
                boat_dict['Length'] = ship_dimensions[0]
                boat_dict['Breadth'] = ship_dimensions[1][:-1]
                continue

        elif field == 'https':
            boat_dict['Photo'] = field + element
            continue

        # Write to dictionary
        boat_dict[field] = element

    for key in selected_columns:
        if key in boat_dict:
            row.append(boat_dict[key])
        else:
            row.append('NULL')

    return row


if __name__ == "__main__":

    driver = webdriver.Chrome(r'C:\Users\Austin\Downloads\chromedriver_win32\chromedriver.exe')

    mmsi_list = ['310627000', '367363350', '311042900', '470992000', '246847000']

    for boat_id in mmsi_list:
        text_data = []
        driver.get(
            'https://www.marinetraffic.com/en/data/?asset_type=vessels&columns=flag,shipname,imo,mmsi,ship_type,show_on_live_map,time_of_latest_position,status,year_of_build,length,width,dwt,callsign&quicksearch|begins|quicksearch=' + boat_id)
        current_url = driver.current_url

        time.sleep(2)

        driver.find_element_by_class_name('ag-cell-content-link').click()

        WebDriverWait(driver, 15).until(EC.url_changes(current_url))

        time.sleep(2)

        text_data.extend(driver.find_element_by_class_name('col-xs-6').text.split('\n'))
        text_data.extend(
            driver.find_element_by_xpath('/html/body/main/div/div/div[1]/div[6]/div[1]/div[1]/div/div[2]').text.split(
                '\n'))
        text_data.extend(driver.find_element_by_id('vessel_details_general').text.split('\n'))
        text_data.append(driver.find_element_by_xpath('//*[@id="big-image"]/img').get_attribute('src'))

        cleaned = boat_text_cleaner(text_data)
        print(cleaned)
