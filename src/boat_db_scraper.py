from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import time
import pandas as pd


def boat_text_cleaner(boat_text_list):

    clean_data = []

    for i, element in enumerate(boat_text_list):

        # Check if standard format cell
        if ':' in element:

            # Check if hyperlink column, done cleaning
            if i == 19:
                clean_data.append(element)
                break

            # Only utilize content right of colon, remove whitespace
            else:
                element = element.split(':')[1].strip()

        # Change hyphens to null and skip cleaning
        else:
            element = 'NULL'
            clean_data.append(element)
            continue

        if i == 3:
            element = element.split(' [')[0]
            clean_data.append(element)

        # Remove ton units
        elif i == 6:
            element = element[:-2]
            clean_data.append(element)

        # Split ship dimensions into two columns
        elif i == 7:
            ship_dimensions = element.split('m × ')

            # Length
            clean_data.append(ship_dimensions[0])

            # Breadth
            clean_data.append(ship_dimensions[1][:-1])

        # Clean
        else:
            clean_data.append(element)

    # Select columns of interest
    selected_columns = [clean_data[1], clean_data[0], clean_data[2],
                        clean_data[4], clean_data[3], clean_data[19],
                        clean_data[9], clean_data[4], clean_data[14],
                        clean_data[5], clean_data[6], clean_data[7],
                        clean_data[8], clean_data[20]]

    return selected_columns


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

        print(text_data)

        # cleaned = boat_text_cleaner(text_data)
        # print(cleaned)
