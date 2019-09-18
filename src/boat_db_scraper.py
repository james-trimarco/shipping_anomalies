from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.common.exceptions import NoSuchElementException
import time
import pandas as pd

def boat_db_scraper(engine, boat_id):
    """
    Scrapes preliminary text from boat database.
    Parameters:
    engine: Selenium WebDriver
        Local chrome webdriver for Selenium scraping
    boat_id: string
        Unique boat MMSI value
    Returns:
    boat_text_list: list
        Preliminary fields from boat db scrape
    """

    # Initialize list to hold scraped text
    boat_text_list = []

    # Navigate to website for boat_id
    engine.get(
        'https://www.marinetraffic.com/en/data/?asset_type=vessels&columns=flag,shipname,imo,mmsi,ship_type,show_on_live_map,time_of_latest_position,status,year_of_build,length,width,dwt,callsign&quicksearch|begins|quicksearch='
        + str(boat_id))

    # Save url and wait for load
    current_url = engine.current_url
    time.sleep(1.3)

    # Try to click boat link
    try:
        boat_link = engine.find_element_by_xpath(
            '//*[@id="borderLayout_eGridPanel"]/div[1]/div/div/div[3]/div[1]/div/div[1]/div[3]/div/div/a')
        boat_link.click()

    # Boat link wasn't present
    except NoSuchElementException:

        # Try to find no boats on page response
        try:
            no_boat_text = engine.find_element_by_xpath('//*[@id="borderLayout_eGridPanel"]/div[2]/div/div/span').text

            # If no boats, return
            if no_boat_text[0:2] == 'No':

                boat_text_list.append('MMSI:' + str(boat_id))

                return boat_text_list

            # Absent boat warning NOT present: Something else is wrong
            else:

                # Try to reload page and click with larger grace period
                try:
                    engine.get(
                        'https://www.marinetraffic.com/en/data/?asset_type=vessels&columns=flag,shipname,imo,mmsi,ship_type,show_on_live_map,time_of_latest_position,status,year_of_build,length,width,dwt,callsign&quicksearch|begins|quicksearch='
                        + str(boat_id))

                    current_url = engine.current_url
                    time.sleep(3)

                    boat_link = engine.find_element_by_xpath(
                        '//*[@id="borderLayout_eGridPanel"]/div[1]/div/div/div[3]/div[1]/div/div[1]/div[3]/div/div/a')

                    boat_link.click()

                # All methods have failed, boat not in database
                except NoSuchElementException:
                    boat_text_list.append('MMSI:' + str(boat_id))
                    return boat_text_list

        # No boat message is missing
        except NoSuchElementException:

            # Try to reload page and click with larger grace period
            try:
                engine.get(
                    'https://www.marinetraffic.com/en/data/?asset_type=vessels&columns=flag,shipname,imo,mmsi,ship_type,show_on_live_map,time_of_latest_position,status,year_of_build,length,width,dwt,callsign&quicksearch|begins|quicksearch='
                    + str(boat_id))

                current_url = engine.current_url
                time.sleep(3)

                boat_link = engine.find_element_by_xpath(
                    '//*[@id="borderLayout_eGridPanel"]/div[1]/div/div/div[3]/div[1]/div/div[1]/div[3]/div/div/a')

                boat_link.click()

            # All methods have failed, boat not in database
            except NoSuchElementException:
                boat_text_list.append('MMSI:' + str(boat_id))
                return boat_text_list

    # Pause until url changes with load
    WebDriverWait(engine, 15).until(ec.url_changes(current_url))
    time.sleep(1)

    # Scrape left data column
    try:
        boat_text_list.extend(engine.find_element_by_class_name('col-xs-6').text.split('\n'))
    except NoSuchElementException:
        pass

    # Scrape right data column
    try:
        boat_text_list.extend(
            engine.find_element_by_xpath('/html/body/main/div/div/div[1]/div[6]/div[1]/div[1]/div/div[2]').text.split(
                '\n'))
    except NoSuchElementException:
        pass

    # Scrape general tab
    try:
        boat_text_list.extend(engine.find_element_by_id('vessel_details_general').text.split('\n'))
    except NoSuchElementException:
        pass

    # Scrape extra name field as fall-back
    try:
        boat_text_list.append('Name: ' + engine.find_element_by_xpath(
            '/html/body/main/div/div/div[1]/div[5]/div/div[2]/div[1]/div[1]/h1').text)
    except NoSuchElementException:
        pass

    # Scrape photo hyperlink if present
    try:
        boat_text_list.append(engine.find_element_by_xpath('//*[@id="big-image"]/img').get_attribute('src'))
    except NoSuchElementException:
        boat_text_list.append('Photo:NULL')

    return boat_text_list


def boat_text_cleaner(boat_text_list):
    """
    Cleans text incoming from Selenium boat db scrape.
    Parameters:
    boat_text_list: list
        List containing text from boat database Selenium scrape
    Returns:
    pandas_df_input: dict
        Records to be uploaded to boat entity table in Postgres
    """

    # Fields of interested located on website
    selected_columns = ['MMSI', 'IMO', 'Call Sign', 'Name', 'Flag',
                        'Home port', 'Year Built', 'AIS Vessel Type',
                        'Vessel Type', 'Gross Tonnage', 'Deadweight',
                        'Length', 'Breadth', 'Photo']

    # Initialize empty list and dictionary for CSV and/or dataframe
    row = []
    boat_dict = {}

    # Check if any data was scraped
    if len(boat_text_list) > 0:

        # Iterate through each feature
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

    # Create list of features
    for key in selected_columns:
        if key in boat_dict:
            row.append(boat_dict[key])
        else:
            row.append('NULL')

    # Turn list into pandas series
    pandas_df_input = dict(zip(selected_columns, row))

    return pandas_df_input


if __name__ == "__main__":

    # Adblocker for quicker page loads: https://www.crx4chrome.com/crx/31931/
    options = webdriver.ChromeOptions()
    options.add_extension(r'C:\Users\Austin\Downloads\extension_1_22_2_0.crx')

    # Start chrome
    driver = webdriver.Chrome(r'C:\Users\Austin\Downloads\chromedriver_win32\chromedriver.exe', options=options)

    # Example boat list to scrape from
    mmsi_list = ['310627000', '367363350', '311042900', '470992000', '246847000']

    # Initialize list to store scraped dictionaries
    pandas_series_list = []

    # Iterate through unique boat MMSI list - REQUIRES MODIFICATION FOR CSV READ
    for unique_boat in mmsi_list:
        # Scrape website
        preliminary_text = boat_db_scraper(driver, unique_boat)

        # Clean scraped data
        cleaned_data = boat_text_cleaner(preliminary_text)

        # Append cleaned data to list
        pandas_series_list.append(cleaned_data)

    # Create dataframe
    df = pd.DataFrame.from_dict(pandas_series_list)
