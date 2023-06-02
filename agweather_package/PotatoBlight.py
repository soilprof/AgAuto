"""
Created on Fri May 31 9:00:00 2019

@author: CAmao

Purpose: PotatoBlight contains the necessary functions to calculate all station DSVs listed in
'2018 Permanent Stations.xlsx'.

Date modified: Fri May 31 2019
"""

from .UsefulClasses import WeatherStation
from .UsefulFunctions import get_path_dir
from .UsefulFunctions import download_file
from .UsefulFunctions import split_text_file
from datetime import datetime
from tqdm import tqdm
import csv

"""
Purpose: This function downloads mawp15.txt from the mbag website, parses the data it finds and organizes it
by station name.
"""


def initialize_stations():
    # Download mawp15.txt into the input_data folder.
    download_file('https://mbagweather.ca/partners/win/mawp15.txt', 'mawp15.txt')
    # We split the text file by '\n' in order to iterate over each line.
    data = split_text_file('mawp15.txt')
    stations_dict = {}
    # We need station_ids as a set because station ids are always unique.
    station_ids = set()
    size = len(data)

    # Iterate over each line in the text file. We wrap 'data' with tqdm in order to generate a progress bar.
    for each in tqdm(data, desc="Calculating station DSVs", total=size):
        data_list = each.strip('\n').split(',')
        # Station ID is the second data point after the date.
        station_id = data_list[1]

        # Check if station ID, temperature, and RH contain invalid values. If they do, then skip this data point.
        if station_id != '-7999' and data_list[2] != '-7999' and data_list[3] != '-7999':
            # Check if the WeatherStation object has already been created.
            if station_id not in station_ids:
                station_ids.add(station_id)
                new_obj = WeatherStation(station_id)
                new_obj.add_data(data_list)
                stations_dict[station_id] = new_obj
            # If WeatherStation for that station ID has been created then simply add the data to the station's list.
            else:
                # Append the data towards the end of the station's data list.
                stations_dict[station_id].add_data(data_list)
    return stations_dict


"""
Purpose: show_all_stations_dsv takes all WeatherStation objects from initialize_stations and analyzes the daily
data stored within each one. The function calculates cumulative and daily DSV for each WeatherStation
based on a specified seed date of the format YYYY-MM-DD.
"""


def show_all_stations_dsv():
    user_date = input("\nPlease specify a \"seed\" date (YYYY-MM-DD):")

    # Use initialize_stations to get us the dictionary of WeatherStation objects.
    stations = initialize_stations()
    # Create/overwrite the comparison.txt file.
    comparison_file = open(get_path_dir('raw_output_data', 'comparison.txt'), 'w+')

    output_txt = ""

    # Create/overwrite the station_dsv.csv file.
    csv_file = open(get_path_dir('raw_output_data', 'station_dsv.csv'), 'w', newline='')

    csv_obj = csv.writer(csv_file, delimiter=',')

    # Write the headers first.
    csv_obj.writerow(['Station', 'Cumulative DSV', 'Today DSV'])
    # Iterate through each WeatherStation object.
    for each in stations.values():
        # If WeatherStation.invalid_data_flag is True then warn the user.
        if each.invalid_data_flag:
            print("Station %s flagged for invalid data. May have skipped some days for this station." % each.get_id())
        # Calculate the daily dsv and cumulative dsv, and get calculations.
        daily_dsv, cumul_dsv, new_txt = each.today_dsv(datetime.strptime(user_date.strip(), '%Y-%m-%d'))
        output_txt += new_txt
        csv_obj.writerow([each.get_id(), cumul_dsv, daily_dsv])
    comparison_file.write(output_txt)
    comparison_file.close()
    csv_file.close()

