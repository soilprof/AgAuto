"""
Created on Fri May 31 9:00:00 2019

@author: CAmao

Purpose: PotatoBlight contains the necessary functions to calculate all station DSVs listed in
'2018 Permanent Stations.xlsx'.

Date modified: Fri May 31 2019
"""

from datetime import datetime, timedelta
from .UsefulFunctions import wisdom_dsv_lookup
from .UsefulFunctions import tomcast_dsv_lookup

# CONSTANTS
MAXIMUM_PERIOD_SIZE = 96
MIN_ALLOWABLE_PERIOD_SIZE = 86
RH_CUTOFF = 86
WISDOM_DSV_CUTOFF = 18
WISDOM_LOW_TEMP_CUTOFF = 7
TOMCAST_LOW_TEMP_CUTOFF = 9
TOMCAST_HIGH_TEMP_CUTOFF = 27
DATE_INDEX = 0
ID_INDEX = 1
TEMP_INDEX = 2
RH_INDEX = 3
RAIN_INDEX = 4
AVG_WS_INDEX = 5
AVG_WD_INDEX = 6

"""
Purpose: Packet serves as the parent class for all classes that contain data within lists and requires an
ID to identify the data. For example, a weather station will have an ID of WEA and its data may contain
a list of all precipitation values for a certain day.

Variables:
    - id: Serves as the identifier that distinguishes one Packet object from another.
    - data: Stores all data values as a list.
    - data_size: The length of the data list.
"""


class Packet(object):

    def __init__(self, name):
        self.id = name
        self.data = []
        self.data_size = 0

    def get_data(self):
        return self.data

    def get_size(self):
        return self.data_size

    def get_id(self):
        return self.id


"""
Purpose: The class DailyData represents each day's data for a station, as outlined in mawp15.txt. 

Variables:
    - date_var: Stands for 'date variable', this is a Date object that stores the date that a DailyData object
    is concerned with.
    - data: Stores all data entries for a certain time period from mawp15.txt. The format for the data is
    [DateTime, StnID, Temp, RH, Rain, AvgWS, AvgWD].
    - period_size: Represents the length of the data list. This means that this is the number of data entries
    for a certain day.
    - avg_temp: This is the average temperature of all data entries that meet the criteria for the Wisdom or
    TomCast DSV look-up tables.
    - period_count: This is the number of data entries that meet the criteria for the Wisdom or TomCast DSV
    look-up tables.
"""


class DailyData:

    def __init__(self, date_var):
        self.date_var = date_var
        self.data = []
        self.period_size = 0
        self.avg_temp = 0.0
        self.period_count = 0

    """
    Purpose: The class function add_data adds a data entry to the data variable and increments the period_size by 1.
    """
    def add_data(self, time_stamp, temp, RH, rain, avg_ws, avg_wd):
        self.data.append([time_stamp, temp, RH, rain, avg_ws, avg_wd])
        self.period_size += 1

    def get_date(self):
        return self.date_var

    """
    Purpose: The class function get_earliest_data returns the date of the first data entry.
    """
    def get_earliest_date(self):
        return self.data[0][0]

    """
    Purpose: The class function get_daily_dsv calculates the DSV based on its list of data entries and the specified
    cumulative DSV value.
    """
    def get_daily_dsv(self, cumul_dsv):
        dsv = 0
        # If missing 10 entries of data then ignore this day and output dsv = 0.
        if self.period_size > MIN_ALLOWABLE_PERIOD_SIZE:
            if cumul_dsv < WISDOM_DSV_CUTOFF:
                # param represents [avg_temp, period_count]
                params = self.wisdom_params()
                self.period_count = params[0]
                self.avg_temp = params[1]
                # based on avg_temp and period_count determine the DSV based on the Wisdom model.
                dsv = wisdom_dsv_lookup(params[0], params[1])
            else:
                params = self.tomcast_params()
                self.period_count = params[0]
                self.avg_temp = params[1]
                # based on avg_temp and period_count determine the DSV based on the TomCast model.
                dsv = tomcast_dsv_lookup(params[0], params[1])
        return dsv

    """
    Purpose: The class function wisdom_params calculates the number of periods that match the criteria
    of a valid data entry for the Wisdom look-up table, as well as the average temperature of those valid
    data periods.
    """
    def wisdom_params(self):
        matching_periods = 0
        temp_sum = 0.0
        for each_entry in self.data:
            if each_entry[1] >= WISDOM_LOW_TEMP_CUTOFF and each_entry[2] >= RH_CUTOFF:  # >= RH_CUTOFF:
                matching_periods += 1
                temp_sum += each_entry[1]
        # If/Else statements are necessary to avoid dividing by 0 if no matching periods are valid.
        if matching_periods == 0:
            temp_sum = 0
        else:
            temp_sum = temp_sum / matching_periods
        # Return a list of # of matching_periods and average temperature.
        return [matching_periods, temp_sum]

    """
    Purpose: The class function wisdom_params calculates the number of periods that match the criteria
    of a valid data entry for the TomCast look-up table, as well as the average temperature of those valid
    data periods.
    """
    def tomcast_params(self):
        matching_periods = 0
        temp_sum = 0.0
        for each_entry in self.data:
            if each_entry[2] >= RH_CUTOFF and (TOMCAST_LOW_TEMP_CUTOFF <= each_entry[1] < TOMCAST_HIGH_TEMP_CUTOFF):
                matching_periods += 1
                temp_sum += each_entry[1]
        if matching_periods == 0:
            temp_sum = 0
        else:
            temp_sum = temp_sum / matching_periods
        return [matching_periods, temp_sum]


"""
Purpose: The class WeatherStation is a child of the Packet class. For the purposes of Potato Blight DSV calculation, it 
represents a WeatherStation and all its daily 15 minute data from mawp15.txt. Each daily data is represented by a
DailyData object.

Variables:
    - header: is simply a list of column names in order to make the output csv easier to read.
    - invalid_data_flag: True if the WeatherStation has days that were skipped (i.e. there were lots of invalid data)
    and stays False if otherwise.
    - data_size: the number of DailyData objects that a WeatherStation object currently has.
    - output_txt: will later store a formatted version of data within a WeatherStation.
"""


class WeatherStation(Packet):

    def __init__(self, name):
        super(WeatherStation, self).__init__(name)
        self.header = [["DateTime", "Temp", "RH", "Rain", "AvgWS", "AvgWD"]]
        self.invalid_data_flag = False
        self.data_size = 0
        self.output_txt = ""

    """
    Purpose: The class function add_data's job is to take a list of the data values of a 15 minute time period and
    insert it to the proper DailyData object.
    
    Parameters:
        - items: list of data values of 15 minute data of the format: [DateTime, StnID, Temp, RH, Rain, AvgWS, AvgWD].
    """
    def add_data(self, items):
        # This If statement allows program to run without error in the event of an EOL character.
        if len(items) > 1:

            try:
                # Creates a Date object based on the time stamp.
                date_info = datetime.strptime(items[DATE_INDEX], '%Y-%m-%d %H:%M')

                temp = float(items[TEMP_INDEX])
                RH = int(items[RH_INDEX])
                rain = float(items[RAIN_INDEX])
                avg_ws = float(items[AVG_WS_INDEX])
                avg_wd = float(items[AVG_WD_INDEX])

                # If the data list is empty then create new DailyData object and add new data entry to it.
                if self.data_size == 0:  # and date_to_hours(date_info) == 12.25:
                    self.add_date(date_info + timedelta(days=1))
                    self.data[-1].add_data(date_info, temp, RH, rain, avg_ws, avg_wd)
                elif self.data_size > 0:
                    if self.check_valid_range(self.data[-1].get_earliest_date(), date_info):
                        self.data[-1].add_data(date_info, temp, RH, rain, avg_ws, avg_wd)
                    # If time stamp is not within the valid range then assume you have to start a new DailyData object.
                    else:
                        if self.data[-1].period_size <= MIN_ALLOWABLE_PERIOD_SIZE and not self.invalid_data_flag:
                            self.invalid_data_flag = True
                        # Create new DailyData object with new date and add data entry to it.
                        self.add_date(date_info + timedelta(days=1))
                        self.data[-1].add_data(date_info, temp, RH, rain, avg_ws, avg_wd)  # Add 12:15 PM data

            except ValueError:
                print("Station data is invalid for %s. Skipping data entry for this time period." % self.id)

    def add_date(self, date_info):
        new_day = DailyData(date_info)
        self.data.append(new_day)
        self.data_size += 1

    """
    Purpose: The purpose of today_dsv_package is to calculate the cumulative dsv of a weather station based on a 
    specified seed date up until today's date or whatever the latest date is stored in a WeatherStation object.
    
    Parameter:
        - seed_date: A user-specified seed-date that tells the program when a farmer has started seeding their
        potatoes.
    """
    def today_dsv_package(self, seed_date):  # Create function to get just today's DSV as well.
        # Get the position of the DailyData object with the same date as seed_date
        index = self.get_date_index(seed_date)
        cumul_dsv = 0
        # Loop through the data list starting at index.
        for each_day in self.data[index:]:
            # Get the daily dsv based on the cumulative dsv.
            daily_dsv = each_day.get_daily_dsv(cumul_dsv)
            cumul_dsv += daily_dsv
            self.output_txt = self.output_txt + ("Station: %s | Date: %s | Daily DSV: %s | Cumulative DSV: %s | Count: %s | Avg. Temp: %.2f\n"
                                                 % (self.id, datetime.strftime(each_day.get_date(), "%Y-%m-%d"), daily_dsv,
                                                       cumul_dsv, each_day.period_count, each_day.avg_temp))

        return cumul_dsv, self.output_txt

    """
    Purpose: The class function today_dsv's purpose is to calculate the DSV for the latest date on the data
    list based on a user-specified seed-date.
    """
    def today_dsv(self, seed_date):  # What about for beginning of season where you have less than 1 day of data?!

        cumul_dsv, output_txt = self.today_dsv_package(seed_date)

        # If the latest DailyData object has a complete data set.
        if self.data[-1].period_size == MAXIMUM_PERIOD_SIZE:  # Maybe change this to MINIMUM_ALLOWABLE_PERIOD_SIZE?
            today_dsv = self.data[-1].get_daily_dsv(cumul_dsv)
        # If the latest DailyData object has an incomplete data set then access the second last DailyData object.
        else:
            today_dsv = self.data[-2].get_daily_dsv(cumul_dsv)  # What about when this object is also incomplete?

        return today_dsv, cumul_dsv, output_txt

    """
    Purpose: get_date_index returns the position of the DailyData object with a date that matches the seed date.
    """
    def get_date_index(self, seed_date):
        index = 0
        for each_day in self.data:
            if each_day.get_date().date() == seed_date.date():
                break
            index += 1
        return index

    def check_valid_range(self, daily_date, new_date):
        daily_date_reset = datetime.strptime(daily_date.strftime("%Y-%m-%d") + " 12:00", "%Y-%m-%d %H:%M") + timedelta(days=1)
        return new_date <= daily_date_reset


# An extension of the standard Python dictionary.
# Allows you to maintain a list for every dictionary key. Useful for organizing different keys that store similar data.
class GroupedArray:

    def __init__(self, identifiers=None, is_scalar=False):
        self.data_dict = {}
        # If no identifiers is specified then just initialize an empty list.
        if identifiers is None:
            identifiers = []
        self.identifiers = identifiers

        # Initialize an empty list for each key in identifiers.
        for each_identifier in identifiers:
            self.data_dict[each_identifier] = []

        self.size = len(identifiers)
        self.is_scalar = is_scalar

    def add_identifier(self, identifier):

        if identifier not in self.data_dict.keys():
            self.identifiers.append(identifier)
            self.data_dict[identifier] = []

    def insert_data(self, identifier, data):

        self.add_identifier(identifier)
        if self.is_scalar:
            self.data_dict[identifier].append(data)
            self.size += 1
        else:
            if isinstance(data, list):
                self.data_dict[identifier].append(data)
                self.size += 1
            else:
                raise Exception('Expected data of Type: List.')

    def get_data(self, identifier):
        return self.data_dict[identifier]

    def get_identifiers(self):
        return self.identifiers


class BatchFile:

    def __init__(self, working_directory):
        self.working_directory = working_directory
        self.batch_contents = "cd %s\n" % working_directory

    def insert_command(self, command):
        self.batch_contents = self.batch_contents + command + '\n'

    def export(self, file_name, folder_path="SELF"):
        if folder_path == 'SELF':
            file_path = self.working_directory + '\\' + file_name
        else:
            file_path = folder_path + '\\' + file_name
        with open(file_path, 'w+') as output_batch:
            output_batch.write(self.batch_contents)
