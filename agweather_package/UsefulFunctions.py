import os
import csv
import requests
from tqdm import tqdm
from datetime import  datetime, timedelta


"""
Purpose: The get_path_dir is responsible for returning a string of a valid file path to a file in the AgAuto cwd if
given a valid directory within the AgAuto cwd and a file within the cwd.

Parameters:
    - directory: Must be a folder that exists within the AgAuto cwd.
    - file_name: The file that you want to access within directory.
    - create: If True, then get_path_dir will not care that the file doesn't exist in directory yet as it assumes
    it will be created using the file path that get_path_dir returns.
"""


def get_path_dir(directory, file_name, create=True):
    # Gets the path of the working directory (i.e. AgAuto's working directory).
    cwd = os.getcwd()
    # Add directory to the working directory path.
    file_base_dir = os.path.join(cwd, directory)
    # Add file_name to the new path created above.
    file_path = os.path.join(file_base_dir, file_name)

    # If the directory doesn't exist then raise an Exception.
    if not os.path.exists(file_base_dir):
        raise Exception('Directory %s does not exist within working directory.' % directory)
    # Raise an exception only if the user specifies create = False. Otherwise, assume they will create after.
    if not create:
        if not os.path.exists(file_path):
            raise Exception('File %s does not exist within %s.' % (file_name, directory))

    return file_path


# Downloads a file if given a url and file_name and stores it in the local PC.
def download_file(url, file_name, default_folder='input_data'):
    # Get the response from URL.
    with requests.get(url, stream=True) as r:
        chunkSize = 1024  # Download 1024 bytes at a time.
        with open(get_path_dir(default_folder, file_name), 'wb') as raw_file:
            for chunk in tqdm(iterable=r.iter_content(chunk_size=chunkSize), total=int(r.headers['Content-Length']) / chunkSize, unit='KB', desc="Downloading %s" % file_name):
                raw_file.write(chunk)


# Takes a text file and returns a list of each line.
def split_text_file(file_name, default_folder='input_data', start_index=1, end_index=-1):

    raw_file = open(get_path_dir(default_folder, file_name), 'r')
    output_text = raw_file.read().split('\n')[start_index:end_index]
    raw_file.close()

    return output_text


def date_to_hours(date_var):
    return date_var.hour + date_var.minute/60.0


# Returns DSV if given number of periods that RH >= 86 and temperature > 7 C based on the Wisdom table.
def wisdom_dsv_lookup(period_count, avg_temperature_raw):

    if avg_temperature_raw > 7.0:
        avg_temperature = round(avg_temperature_raw)
    else:
        avg_temperature = avg_temperature_raw

    dsv = 0
    if 0 <= period_count < 39:
        dsv = 0
    elif (39 <= period_count <= 50) and (avg_temperature < 15.5):
        dsv = 0
    elif (39 <= period_count <= 50) and (avg_temperature >= 15.5):
        dsv = 1
    elif (51 <= period_count <= 62) and (avg_temperature < 12.5):
        dsv = 0
    elif (51 <= period_count <= 62) and (12.5 <= avg_temperature < 15.5):
        dsv = 1
    elif (51 <= period_count <= 62) and (avg_temperature >= 15.5):
        dsv = 2
    elif (63 <= period_count <= 74) and (avg_temperature < 12.5):
        dsv = 1
    elif (63 <= period_count <= 74) and (12.5 <= avg_temperature < 15.5):
        dsv = 2
    elif (63 <= period_count <= 74) and (avg_temperature >= 15.5):
        dsv = 3
    elif (75 <= period_count <= 86) and (avg_temperature < 12.5):
        dsv = 2
    elif (75 <= period_count <= 86) and (12.5 <= avg_temperature < 15.5):
        dsv = 3
    elif (75 <= period_count <= 86) and (avg_temperature >= 15.5):
        dsv = 4
    elif (87 <= period_count <= 96) and (avg_temperature < 12.5):
        dsv = 3
    elif (87 <= period_count <= 96) and (avg_temperature >= 12.5):
        dsv = 4

    return dsv


# Returns DSV if given number of periods that RH >= 86 and temperature > 7 C based on the Tomcast table.
def tomcast_dsv_lookup(period_count, avg_temperature_raw):

    if avg_temperature_raw > 9.0:
        avg_temperature = round(avg_temperature_raw)
    else:
        avg_temperature = avg_temperature_raw

    dsv = 0
    if 0 <= period_count <= 10:
        dsv = 0
    elif (11 <= period_count <= 14) and (avg_temperature < 20.5):
        dsv = 0
    elif (11 <= period_count <= 14) and (20.5 <= avg_temperature < 25.5):
        dsv = 1
    elif (11 <= period_count <= 14) and (avg_temperature >= 25.5):
        dsv = 0
    elif (15 <= period_count <= 22) and (avg_temperature < 17.5):
        dsv = 0
    elif (15 <= period_count <= 22) and (avg_temperature >= 17.5):
        dsv = 1
    elif (23 <= period_count <= 26) and (avg_temperature < 17.5):
        dsv = 0
    elif (23 <= period_count <= 26) and (17.5 <= avg_temperature < 20.5):
        dsv = 1
    elif (23 <= period_count <= 26) and (20.5 <= avg_temperature < 25.5):
        dsv = 2
    elif (23 <= period_count <= 26) and (avg_temperature >= 25.5):
        dsv = 1
    elif (27 <= period_count <= 34) and (avg_temperature < 20.5):
        dsv = 1
    elif (27 <= period_count <= 34) and (20.5 <= avg_temperature < 25.5):
        dsv = 2
    elif (27 <= period_count <= 34) and (avg_temperature >= 25.5):
        dsv = 1
    elif (35 <= period_count <= 50) and (avg_temperature < 17.5):
        dsv = 1
    elif (35 <= period_count <= 50) and (avg_temperature >= 17.5):
        dsv = 2
    elif (51 <= period_count <= 62) and (avg_temperature < 12.5):
        dsv = 2
    elif (51 <= period_count <= 62) and (12.5 <= avg_temperature < 17.5):
        dsv = 1
    elif (51 <= period_count <= 62) and (17.5 <= avg_temperature < 20.5):
        dsv = 2
    elif (51 <= period_count <= 62) and (20.5 <= avg_temperature < 25.5):
        dsv = 3
    elif (51 <= period_count <= 62) and (avg_temperature >= 25.5):
        dsv = 2
    elif (63 <= period_count <= 82) and (avg_temperature < 17.5):
        dsv = 2
    elif (63 <= period_count <= 82) and (avg_temperature >= 17.5):
        dsv = 3
    elif (83 <= period_count <= 90) and (avg_temperature < 20.5):
        dsv = 3
    elif (83 <= period_count <= 90) and (20.5 <= avg_temperature < 25.5):
        dsv = 4
    elif (83 <= period_count <= 90) and (avg_temperature >= 25.5):
        dsv = 3
    elif (91 <= period_count <= 96) and (avg_temperature < 17.5):
        dsv = 3
    elif (91 <= period_count <= 96) and (avg_temperature >= 17.5):
        dsv = 4

    return dsv


# If given a string representing the cardinal direction, will return equivalent direction in degrees.
# Angle is measured clockwise from the northern direction.
def cardinal_to_degrees(cardinal_dir):
    cardinal_dict = {
        'N': 0, 'NNE': 22.5, 'NE': 45, 'ENE': 77.5, 'E': 90, 'ESE': 112.5, 'SE': 135, 'SSE': 157.5, 'S': 180,
        'SSW': 202.5, 'SW': 225, 'WSW': 247.5, 'W': 270, 'WNW': 292.5, 'NW': 315, 'NNW': 337.5
    }

    try:
        output = cardinal_dict[cardinal_dir]
    except KeyError:
        output = ''

    return output


def write_list_to_csv(file_name, contents_list):
    with open(file_name, 'w', newline='') as csv_file:
        daily_ec = csv.writer(csv_file, delimiter=',')
        for each_row in contents_list:
            daily_ec.writerow(each_row)


def chunks(l, n):
    # For item i in a range that is a length of l,
    for i in range(0, len(l), n):
        # Create an index range for l of n items:
        yield l[i:i+n]


def increment_all_date_str(date_str_list, increment, string_format):
    for each_index in range(len(date_str_list)):
        date_obj = datetime.strptime(date_str_list[each_index], string_format) + timedelta(days=increment)
        date_str_list[each_index] = date_obj.strftime(string_format)
