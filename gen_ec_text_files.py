from agweather_package import xml_parser as parse
from agweather_package import get_path_dir
from agweather_package import BatchFile
from os import getcwd
from subprocess import call


def main():
    daily_data_array = parse.grab_desired_xml_data('daily')
    hourly_data_array = parse.grab_desired_xml_data('hourly')
    id_dictionary = parse.station_id_dictionary('mbag_id')
    desc_dictionary = parse.station_id_dictionary('desc')
    batch_file = BatchFile(getcwd())
    batch_file.insert_command('cd output_files')

    for each_id in id_dictionary.keys():
        file_name_60 = desc_dictionary[each_id] + '60' + '.txt'
        file_name_24 = desc_dictionary[each_id] + '24' + '.txt'
        with open(get_path_dir('output_files', file_name_24), 'w+') as daily_file:
            daily_file.write(parse.gen_string_rep(daily_data_array.get_data(each_id)))

        with open(get_path_dir('output_files', file_name_60), 'w+') as hourly_file:
            hourly_file.write(parse.gen_string_rep(hourly_data_array.get_data(each_id)))

        batch_file.insert_command('copy %s C:\\WWW\\live.mbagweather.ca\\ec_files' % file_name_60)
        batch_file.insert_command('del %s' % file_name_60)
        batch_file.insert_command('copy %s C:\\WWW\\live.mbagweather.ca\\ec_files' % file_name_24)
        batch_file.insert_command('del %s' % file_name_24)

    batch_file.export('move_ec_texts.bat')
    call('move_ec_texts.bat')


main()
