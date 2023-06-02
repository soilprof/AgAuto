from bs4 import BeautifulSoup
from operator import itemgetter
from xml.etree import ElementTree
import urllib.request
import csv
import yaml
from .UsefulFunctions import get_path_dir
from .UsefulClasses import GroupedArray
from .UsefulFunctions import cardinal_to_degrees
from tqdm import tqdm

"""
These functions are used to collect SWOB-ML data from the Environment Canada
Datastore.  The data can be organized into CSV and Excel formats using the function within.

See the 'main' section of this file for examples
"""


MD_IE_PATH = '{http://dms.ec.gc.ca/schema/point-observation/2.1}set/' \
             '{http://dms.ec.gc.ca/schema/point-observation/2.1}identification-elements'
R_ELEMENTS_PATH = '{http://dms.ec.gc.ca/schema/point-observation/2.1}elements'
HOURLY_FIELDS = ['air_temperature', 'humidex', 'wind_speed', 'wind_direction']
DAILY_FIELDS = ['air_temperature_yesterday_high', 'air_temperature_yesterday_low',
                'total_precipitation', 'wind_gust_speed',
                'record_high_temperature', 'record_high_temperature_year',
                'record_low_temperature', 'record_low_temperature_year']
NUMBER_OF_DAILY = 2
NUMBER_OF_HOURLY = 48
HOURLY_URL = 'http://dd.weather.gc.ca/observations/xml/MB/hourly/'
DAILY_URL = 'http://dd.weather.gc.ca/observations/xml/MB/yesterday/'


def get_html_string(url):
    """
    Gets the html string from a url
    :param url: (str) the url to get html from
    :returns: (str) the string representation of the html at a url
    """
    catcher = 0
    while catcher < 3:
        try:
            URLObj = urllib.request.urlopen(url)
            html_string = URLObj.read().decode('utf-8')
            catcher = 3
            return html_string
        except:
            print("Link retrieval error on:")
            print(url)
            html_string = ""
            catcher += 1
            if catcher == 3:
                return html_string
            else:
                print("Trying again")


def get_stations_list(urlroot, strdate):
    """
    Returns a list of the all stations for which swob-ml observations are available
    :param urlroot: (str) the root url to base searches from 
    :param strdate: (str) the date string in YYYYMMDD format
    :returns: (list) of str with 3 letter station designations
    """
    all_stations_list = []
    all_stations_html = get_html_string(urlroot+strdate+"/")
    all_stations_soup = BeautifulSoup(all_stations_html, features="html5lib")

    for tag in all_stations_soup.findAll('a', href=True):
        # length is 5: eg. "CVSL/", we remove the "/" to get station name
        if tag['href'].__len__() == 5:
            tag['href'] = tag['href'].replace("/", "")
            tag['href'] = tag['href'][1:].encode('ascii', 'ignore')
            all_stations_list.append(tag['href'])
    
    return all_stations_list


def clean_incoming(clean_info_filename="in.txt", default_order=500):
    """
    Creates an index from which to sort data.  Indexable by field_name and includes whether or not to override
    field_name with a human specified readable field name and desired order.
    :param clean_info_filename: (str optional) the filename of the text file to use for creating the output dictionary
           This file should be formated with csv data as such: 
           "fieldx_name, Readable Field Name, (int) order\n
            fieldx+1_name, Readable Field Name, (int) order\n" where each 3 value sequence represents
           a field and is on its own line.
           Default: 'in.txt'
    :param default_order: (int optional) the desired default order for fields to appear in outputs in.
           Default: 500
    :returns: (dict, bool) where the dict is a dictionary of 
           {"field_name":["Readable Field Name",(int) Priority],...} format
           The bool returned is True if data should be cleaned using this information, or False otherwise
    """
    try:
        clean_info = {}
        clean_info_file_obj = open(get_path_dir("config_files", clean_info_filename), 'r')
        split = csv.reader(clean_info_file_obj, delimiter=',', skipinitialspace=True)
        for line_data_list in split:
            if line_data_list.__len__() <= 3:
                clean = True
                line_data_list.append(default_order)
                clean_info[line_data_list[0]] = [line_data_list[1], line_data_list[2]]
                # clean_info["date_tm"] = ["date_tm", -3]
                # clean_info["tc_id"]   = ["TC ID", -2]
                # clean_info["stn_nam"] = ["Station Name", -1]
                # clean_info["lat"]     = ["lat", -120]
                # clean_info["long"]    = ["long", -120]
                # clean_info["msc/observation/atmospheric/surface_weather/ca-1.0-ascii"]=["mscschema", -100]
        clean_info_file_obj.close()
    except:
        clean = False
        if clean_info_filename != "OFF":
            print("Can't read file passed to clean_incoming()")
    
    return clean_info, clean


def parse_xml_links(link_base_url_root, xml_links, title_dict={}, clean_dict={}, clean=False, default_order=500, default_config="mbag"):
    """
    Parse xml links and collect data from those links
    :param link_base_url_root: (str) the base url from which to add all links
    :param xml_links: (list) of str such that each value is a link to an xml that can be added to link_base_url_root
    :param title_dict: (dict optional) a dictionary in {'field' : [order,uom],...} format for later formatting of field names
           Default: {}
    :param clean_dict: (dict optional) a dictionary of 
           {"field_name":["Readable Field Name",(int) Priority],...} format
           Default: {}
    :param clean: (bool optional) True if data should be cleaned using clean_dict, or False otherwise
           Default: False
    :param default_order: (int optional) the desired default order for fields to appear in outputs in.
           Default: 500
    :returns: (list, list) a list of dicts where each dict is the xml data from one link, and a list of sorted titles
    """
    total_xml_data = []
    if default_config == "mbag":
        xml_links = [xml_links[-2]]  # gets the latest, english file.
    for xml_address in xml_links:
        catcher = 0
        while catcher < 3:
            try:
                # maybe use xml_file as a local file so you don't have to connect to phone wifi.
                xml_file = urllib.request.urlopen(link_base_url_root + xml_address)
                xml_parser_obj = ElementTree.parse(xml_file)
                catcher = 3
            except:
                catcher += 1
                print("Error opening xmladdress" + xml_address)
                
            lastname = ""
            single_xml_data = {}
            el_tree = xml_parser_obj.getiterator()
            for node in el_tree:
                name = node.attrib.get('name')
                value = node.attrib.get('value')
                uom = node.attrib.get('uom').encode('ascii', 'ignore')  # used to be wrapped by unicode()
                order = int(default_order)
                qual = "qa_none"

                if name == "qa_summary":
                    qual = str(value)
                    single_xml_data[lastname][3] = qual
                else:
                    if clean:
                        try:
                            order = int(clean_dict[name][1])
                            # Modify name last (lookups depend on it)
                            name = clean_dict[name][0]
                        except:
                            pass
                        
                    single_xml_data[name] = [value, uom, order, qual]
                    title_dict[name] = [order, uom]
                  
                lastname = name
            total_xml_data.append(single_xml_data)
            title_list_sorted = sorted(title_dict.iteritems(), key=itemgetter(1), reverse=False)

    return total_xml_data, title_list_sorted


def parse_mbag_xml(link_base_url_root, strdate, title_dict={}, clean_dict={}, clean=False, default_order=500):
    total_xml_data = []
    xml_address = "yesterday_mb_%s_e.xml" % strdate
    catcher = 0
    while catcher < 3:
        try:
            # maybe use xml_file as a local file so you don't have to connect to phone wifi.
            xml_file = urllib.request.urlopen(link_base_url_root + xml_address)
            xml_parser_obj = ElementTree.parse(xml_file)
            catcher = 3
        except:
            catcher += 1
            print("Error opening xmladdress" + xml_address)

        el_tree = xml_parser_obj.getroot().getchildren()
        for node in el_tree:
            single_xml_data = {}
            data_nodes = node.getchildren()
            identification_node = data_nodes[0].getchildren()[0].getchildren()[0].getchildren()[-1]
            result_node = data_nodes[-1].getchildren()[-1]
            for each_node in [identification_node, result_node.getchildren()[0]]:
                for each_element in each_node.getchildren():
                    name = each_element.attrib.get('name')
                    value = each_element.attrib.get('value')
                    uom = each_element.attrib.get('uom').encode('ascii', 'ignore')
                    order = int(default_order)
                    qual = "qa_none"

                    if clean:
                        try:
                            order = int(clean_dict[name][1])
                            # Modify name last (lookups depend on it)
                            name = clean_dict[name][0]
                        except:
                            pass

                    single_xml_data[name] = [value, uom, order, qual]
                    title_dict[name] = [order, uom]
            single_xml_copy = dict(single_xml_data)
            total_xml_data.append(single_xml_copy)

        title_list_sorted = sorted(list(title_dict.items()), key=itemgetter(1), reverse=False)

    return total_xml_data, title_list_sorted


# Parses the xml file downloaded from xml_link into an ElementTree object and returns it.
def get_xml_obj(xml_link):
    try:
        xml_file = urllib.request.urlopen(xml_link)
        xml_obj = ElementTree.parse(xml_file)

    except urllib.request.URLError:
        raise Exception("There is something wrong with the URL. Also, am I connected to the ME?")

    return xml_obj


# Gets all nodes with the 'name' specified by identifier.
def get_parent_nodes(xml_obj, identifier):
    parent_nodes = []
    for each in xml_obj.getroot().iter(identifier):
        parent_nodes.append(each)
    return parent_nodes


# Returns a dictionary of all EC stations with corresponding data from stations.yaml.
def station_id_dictionary(key='', all_keys=False):
    output_dict = {}
    with open(get_path_dir('config_files', 'stations.yaml'), 'r') as station_ids:
        yaml_load = yaml.safe_load(station_ids)
        for each_station in yaml_load:
            # If all_keys == True then it stores all data for that station from stations.yaml.
            if all_keys:
                output_dict[each_station] = yaml_load[each_station]
            # Otherwise it just stores a specific value into output_dict.
            else:
                output_dict[each_station] = yaml_load[each_station][key]
    return output_dict


# Gets the value of a field when given an ElementTree object (xml_obj) and a station name.
def get_value(xml_obj, station, field_name):
    metadata = get_parent_nodes(xml_obj, '{http://www.opengis.net/om/1.0}metadata')
    result = get_parent_nodes(xml_obj, '{http://www.opengis.net/om/1.0}result')
    value = ''
    for each_index in range(len(metadata)):
        # Get Identification elements from metadata. Identification elements stores the transport_canada_id.
        # Look inside one of the xml files from http://dd.weather.gc.ca/observations/xml/MB/yesterday/ to understand.
        meta_contents = metadata[each_index].find(MD_IE_PATH).getchildren()
        result_contents = result[each_index].find(R_ELEMENTS_PATH).getchildren()
        tc_id = extract_value(meta_contents, 'transport_canada_id')
        if tc_id == station:
            value = extract_value(result_contents, field_name)
    return value


def get_date_from_xml(xml_obj):
    meta_data = get_parent_nodes(xml_obj, '{http://www.opengis.net/om/1.0}metadata')
    meta_contents = meta_data[-1].find(MD_IE_PATH).getchildren()
    return extract_value(meta_contents, 'observation_date_local_time').split('T')[0]


def update_weather_array(xml_obj, fields, grouped_array):
    metadata = get_parent_nodes(xml_obj, '{http://www.opengis.net/om/1.0}metadata')
    result = get_parent_nodes(xml_obj, '{http://www.opengis.net/om/1.0}result')
    id_dictionary = station_id_dictionary('mbag_id')
    if len(metadata) != len(result):
        raise Exception('List of metadata and result are not the same size!')
    else:
        list_size = len(metadata)
        for each_index in range(list_size):
            meta_contents = metadata[each_index].find(MD_IE_PATH).getchildren()
            result_contents = result[each_index].find(R_ELEMENTS_PATH).getchildren()
            tc_id = extract_value(meta_contents, 'transport_canada_id')
            mbag_id = None
            observation_date = extract_value(meta_contents, 'observation_date_local_time')
            if tc_id in id_dictionary.keys():
                if observation_date is not None:
                    observation_date = extract_value(
                        meta_contents, 'observation_date_local_time').replace('.000 CDT', '').replace('T', ' ')
                    mbag_id = id_dictionary[tc_id]
                data_entry = [observation_date, tc_id, mbag_id]
                for each_field in fields:
                    field_value = extract_value(result_contents, each_field)
                    data_entry.append(field_value)
                grouped_array.insert_data(tc_id, data_entry)


def grab_desired_xml_data(daily_or_hourly):

    weather_grouped_array = GroupedArray()
    if daily_or_hourly == 'daily':
        xml_url = DAILY_URL
        fields = DAILY_FIELDS
        period = 2
        desired_xml_file_names = list_xml_links(xml_url)[-(period + 1):-1]  # Yesterday and the day before yesterday.
    elif daily_or_hourly == 'hourly':
        xml_url = HOURLY_URL
        fields = HOURLY_FIELDS
        period = 48
        desired_xml_file_names = list_xml_links(xml_url)[-period:]
    else:
        raise Exception('Expected \'daily\' or \'hourly\', got %s instead.' % daily_or_hourly)

    for each_file in tqdm(iterable=desired_xml_file_names, total=period, desc='Downloading %s data' % daily_or_hourly):
        xml_obj = get_xml_obj(xml_url + '/' + each_file)
        update_weather_array(xml_obj, fields, weather_grouped_array)

    return weather_grouped_array


def list_xml_links(xml_links_url):
    one_station_html = get_html_string(xml_links_url)
    one_station_xml_links = []
    one_station_soup = BeautifulSoup(one_station_html, features="html5lib")

    for tag in one_station_soup.findAll('a', href=True):
        if ".xml" in tag['href']:
            file_name = str(tag['href'].encode('ascii', 'ignore'), "utf-8")
            if file_name.split('_')[-1] == 'e.xml':
                one_station_xml_links.append(file_name)

    return one_station_xml_links


def extract_value(element_list, identifier, attrib_to_search='name', attrib_for_value='value'):
    value = None
    for each_element in element_list:
        name = each_element.attrib.get(attrib_to_search)
        if name == 'record_high_temperature' and identifier == 'record_high_temperature_year':
            value = each_element.getchildren()[-1].attrib.get(attrib_for_value)
            break
        elif name == 'record_low_temperature' and identifier == 'record_low_temperature_year':
            value = each_element.getchildren()[-1].attrib.get(attrib_for_value)
            break
        elif name == 'wind_direction' and identifier == 'wind_direction':
            value = cardinal_to_degrees(each_element.attrib.get(attrib_for_value))
            break
        elif name == identifier:
            value = each_element.attrib.get(attrib_for_value)
            break

    return value


def gen_string_rep(data_packet):
    string_rep = ""
    for each_entry in data_packet:
        for each_item in each_entry:
            string_rep = string_rep + str(each_item)
            if each_item != each_entry[-1]:
                string_rep = string_rep + ','
        string_rep = string_rep + '\n'

    return string_rep


def generate_daily_xml_link(date):
    url = DAILY_URL + 'yesterday_mb_' + date + '_e.xml'
    return url


def parse_station(urlroot, strdate, station="default", title_dict={}, clean_dict={}, clean=False, default_order=500, default_config="mbag"):
    """
    Parses all station data from a date
    :param urlroot: (str) the url root from which all SWOB-ML dates are listed
    :param strdate: (str) the date in "YYYYMMDD" format to get the station data on
    :param station: (str) the three (or four) character station identifier eg. "VSL"
    :param title_dict: (dict optional) a dictionary in {'field' : [order,uom],...} format for later formatting of field
    names
           Default: {}
    :param clean_dict: (dict optional) a dictionary of 
           {"field_name":["Readable Field Name",(int) Priority],...} format
           Default: {}
    :param clean: (bool optional) True if data should be cleaned using clean_dict, or False otherwise
           Default: False
    :param default_order: (int optional) the desired default order for fields to appear in outputs in.
           Default: 500
    :returns: (list, list) a list of dicts where each dict is the xml data from one hour at the station, and a list of
    sorted titles
    """
    if station.__len__() == 3:
        station = "C" + station
        
    if station == "default":
        one_station_url = "http://dd.weather.gc.ca/observations/xml/MB/yesterday/"
    else:
        one_station_url = urlroot + strdate + "/" + station + "/"

    one_station_html = get_html_string(one_station_url)
    one_station_xml_links = []
    one_station_soup = BeautifulSoup(one_station_html, features="html5lib")
    
    for tag in one_station_soup.findAll('a', href=True):
        if ".xml" in tag['href']:
            file_name = tag['href'].encode('ascii', 'ignore')
            one_station_xml_links.append(file_name)
    
    if default_config == "mbag":
        one_station_data_list, ordered_titles = parse_mbag_xml(one_station_url, strdate, title_dict=title_dict,
                                                               clean_dict=clean_dict, clean=clean)
    else:
        one_station_data_list, ordered_titles = parse_xml_links(one_station_url, one_station_xml_links,
                                                                title_dict=title_dict, clean_dict=clean_dict,
                                                                clean=clean)

    return one_station_data_list, ordered_titles


def order_row(row, ordered_titles):
    """
    Orders an individual row so that it follows the field order of ordered_titles
    :param row: a dict from the results_list
    :param ordered_titles: a list of field title tuples ordered by priority in
        [("fieldx_name", [(int) priority, "unit"]), ("fieldx+1_name", [(int) priority, "unit"]),...] format
        where each tuple in the list is used to order the data in results_list and for the header data.
    :returns: (list) a row as a list with just the data values as columns.  No units or qualifiers are included.
    """
    ordered_row = []
    for name in ordered_titles:
        ordered_row.append(str(row.get(name[0], [""])[0]))
    
    return ordered_row


def order_results(results_list, ordered_titles):
    """
    Orders list results so that they follow the field order of ordered_titles
    :param results_list: a list of station information in 
        [{'fieldx_name':["datum","unit",(int) order,"quality"],'fieldx+1_name':[...]},{...},...] format
        where each dictionary in the list gets rendered as a row
    :param ordered_titles: a list of field title tuples ordered by priority in
        [("fieldx_name", [(int) priority, "unit"]), ("fieldx+1_name", [(int) priority, "unit"]),...] format
        where each tuple in the list is used to order the data in results_list and for the header data. 
    :returns: (list) results from input ordered in order of ordered_titles and with only the value as the field
    """
    results = []
    for row in results_list:
        results.append(order_row(row, ordered_titles))
    
    return results


def finalize_titles(ordered_titles):
    """
    Clean title information for ["Title (unit)",...] format
    :param ordered_titles: a list of field title tuples ordered by priority in
        [("fieldx_name", [(int) priority, "unit"]), ("fieldx+1_name", [(int) priority, "unit"]),...] format
        where each tuple in the list is used to order the data in results_list and for the header data.
    :returns: (list) of str in "Title (unit)" format for use in headers
    """
    titles = []
    for title in ordered_titles:
        titles.append(str(title[0]) + " (" + str(title[1][1]) + ")")
    
    return titles


def csv_out(results_list, ordered_titles, filename):
    """
    Outputs data to a CSV file
    :param results_list: a list of station information in 
        [{'fieldx_name':["datum","unit",(int) order,"quality"],'fieldx+1_name':[...]},{...},...] format
        where each dictionary in the list gets rendered as a row
    :param ordered_titles: a list of field title tuples ordered by priority in
        [("fieldx_name", [(int) priority, "unit"]), ("fieldx+1_name", [(int) priority, "unit"]),...] format
        where each tuple in the list is used to order the data in results_list and for the header data.
    :param filename: (str) the name of the file to write the csv to
    :returns: (bool) True if successful, False otherwise
    """
    try:
        # Sanitizes the result information so it only includes the value
        ordered_results_list = order_results(results_list, ordered_titles)
        # Orders the titles into a string list so it can be added to a csv
        ordered_titles_list = finalize_titles(ordered_titles)
        
        # Write the header and data to the csv file
        with open(get_path_dir('raw_output_data', filename), "w", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(ordered_titles_list)
            writer.writerows(ordered_results_list)
        
        # We were successful
        return True
    except:
        # An error occurred
        return False


"""
A collection of examples on how to use these functions
"""


if __name__ == "__main__":
    """
    # Specify the date in YYYYMMDD format.
    # Here we get the current ZULU date in YYYYMMDD format.
    strdate = datetime.datetime.utcnow().strftime("%Y%m%d")
    # Example of how to get all stations.
    #all_stations = get_stations_list(urlroot, strdate)
    # Get the rules for cleaning and sorting output files.
    clean_dict, clean = clean_incoming("fields.txt")
    # Parse the data for a station.  "VSL" is used in this example.
    results_list, ordered_titles = parse_station(urlroot, strdate, "WCF", clean_dict=clean_dict, clean=clean)
    print len(results_list)
    print len(ordered_titles)
    # Example of how to output data to Excel and CSV formats.
    #excel_out(results_list, ordered_titles, "output.xls")
    csv_out(results_list, ordered_titles, "output.csv")
    """

    all_data = grab_desired_xml_data('daily')
    print(gen_string_rep(all_data.get_data('PBO')))

    station_id_dictionary()

