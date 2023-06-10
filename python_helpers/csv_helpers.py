from collections import OrderedDict
import csv
import json
import re
import os
import xml.etree.ElementTree as ET
import xmltodict



def dict_to_csv(xml_dict: OrderedDict, iteration: int, directory: str):
    csv_name = "dataset"+str(iteration)+".csv"
    keys = []
    for key in xml_dict.items():
        keys.append(key)

    print(keys)
    # with open(directory+"/"+csv_name, 'w') as file:
    #     w = csv.DictWriter(file, xml_dict.keys())
    #     w.writeheader()
    #     w.writerow(xml_dict)

def xml_to_csv(directory: str):
    iteration = 0
    # Parse the XML file
    xml_files = os.listdir(directory)
    print(directory)
    for xml_file in xml_files:
        if xml_file.endswith('.xml'):
            with open(xml_file, 'r') as file:
                tree = ET.parse(file)
                root = tree.getroot()

                print("root: " + str(root[0]))
                # Extract the header row
                headers = []
                for child in root[0]:
                    headers.append(child.tag)

                # Extract the data rows
                rows = []
                for element in root:
                    row = []
                    for child in element:
                        row.append(child.text)
                    rows.append(row)

                csv_name = "dataset"+str(iteration)+".csv"
                csv_path = directory+"/data_downloads/"
                if not os.path.exists(csv_path):
                    os.makedirs(csv_path)
                # Write the data to a CSV file
                with open(csv_path+csv_name, 'w', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow(headers)
                    writer.writerows(rows)
                iteration = iteration + 1

        print("CSV file created successfully.")

#xml_to_csv(os.getcwd())

def xml_to_csv_part_2(directory: str):
    print("listing files...")
    print(directory)
    xml_files = os.listdir(directory)
    print("xml files: ", xml_files)
    iteration = 0
    for xml_file in os.listdir(directory):
        xml_file_path = os.path.join(directory, xml_file)
        print(xml_file)
        if xml_file.endswith('.xml'):
            with open(xml_file_path, 'r') as file:
                print("opening: ", file)
                data_dict = xmltodict.parse(file.read())
                json_data = json.dumps(data_dict)
                csv_name = "dataset"+str(iteration)+".csv"
                csv_file_path = os.path.join(directory, csv_name)
                with open(csv_file_path, 'w') as json_file:
                    json_file.write(json_data)
                    #json_file.close()
                iteration = iteration + 1

def xml_to_csv_part_3(directory: str):
    print("listing files...")
    print(directory)
    xml_files = os.listdir(directory)
    print("xml files: ", xml_files)
    iteration = 0
    for xml_file in os.listdir(directory):
        xml_file_path = os.path.join(directory, xml_file)
        print(xml_file)
        if xml_file.endswith('.xml'):
            with open(xml_file_path, 'r') as file:
                print("opening: ", file)
                xml_dict = xmltodict.parse(file.read())
                header = sorted(set(i for b in map(dict.keys, xml_dict.values()) for i in b))
                csv_name = "dataset"+str(iteration)+".csv"
                csv_file_path = os.path.join(directory, csv_name)
                with open(csv_file_path, 'w') as csv_file:
                    write = csv.writer(csv_file)
                    write.writerow(['wfs_capabilities', *header])
                    for a, b in xml_dict.items():
                        write.writerow([a]+[b.get(i, '') for i in header])
                iteration = iteration + 1

# THIS WORKS ONLY IF U HARDCODE THE ITER AND NAMESPACE AND STRUFF
def xml_to_csv_part_4(directory: str):
    print("listing files...")
    print(directory)
    xml_files = os.listdir(directory)
    print("xml files: ", xml_files)
    iteration = 0
    for xml_file in os.listdir(directory):
        xml_file_path = os.path.join(directory, xml_file)
        print(xml_file)
        if xml_file.endswith('.xml'):
            with open(xml_file_path, 'r', encoding='UTF-8') as file:
                print("opening: ", file)
                root = ET.fromstring(file.read())
                data = []
                namespace = get_namespace(root)
                fields = set()
                print("root: ", root)
                csv_name = "dataset"+str(iteration)+".csv"
                csv_file_path = os.path.join(directory, csv_name)
                for member in root.iter('{http://tree-register}ckan_13079796_1b3a_448d_8a09_d7fda5611c5f'):
                    item = {}
                    for child in member:
                        tag = child.tag.replace('{http://tree-register}', '')
                        fields.add(tag)
                        item[tag] = child.text
                    data.append(item)
                with open(csv_file_path, 'w', newline='') as csv_file:
                        writer = csv.DictWriter(csv_file, fieldnames=sorted(fields))
                        writer.writeheader()
                        writer.writerows(data)



# working version
def xml_to_csv_part_5(directory: str):
    """
    @TODO: Cleanup
    @TODO: Rename function
    @TODO: Maybe add some global variables for the directories and what not.
    @TODO: wrt to the above, can maybe see if passing in list of paths would be better. The list of paths would come from the helpers.py paging function. Could cut out some unnecessary calls.
    @TODO: delete XML files from users directory automatically. Likely after we generate the CSVs.
    @TODO: merge CSVs

    Loop through all the XML's previously downloaded and then turn them into CSV's using built-in functions.

    Args:
        directory: string representing directory of where to find all the XML files. Passed in from helpers.py.
    
    Return:
        Nothing. CSV files are downloaded to local directory.

    """
    print("listing files 5...")
    print(directory)
    xml_files = os.listdir(directory)
    print("xml files 5: ", xml_files)
    iteration = 0
    # for every file in the directory
    for xml_file in os.listdir(directory):
        xml_file_path = os.path.join(directory, xml_file)
        print(xml_file)
        # check if file is an XML file. If so, we need to work with it.
        if xml_file.endswith('.xml'):
            # open the XML file
            with open(xml_file_path, 'r', encoding='UTF-8') as file:
                print("opening: ", )
                # turn it into an XML tree using ElementTree
                root = ET.fromstring(file.read())
                print("root: ", root)
                fields = set()
                data = []

                # open a CSV file in which we will be writing to 
                # @TODO: clean this up. Can open CSV file after getting the data collected in the list.
                csv_name = "dataset"+str(iteration)+".csv"
                csv_file_path = os.path.join(directory, csv_name)
                with open(csv_file_path, 'w', newline='') as f:
                    actual_root = None

                    for child in root.find('.//'):
                        actual_root = child.tag
                    print("actual root: ", actual_root)
                    namespace = get_namespace_no_tag(actual_root)
                    print("namespace: ", namespace)
                    # Write the data rows
                    for member in root.iter(actual_root):
                        item = {}
                        for child in member:
                            tag = child.tag.replace(f'{namespace}', '')
                            fields.add(tag)
                            item[tag] = child.text
                        data.append(item)
                    iteration = iteration + 1

                    # once data is populated, write it to the CSV file with the appropriate headers
                    writer = csv.DictWriter(f, fieldnames=sorted(fields))
                    writer.writeheader()
                    writer.writerows(data)


                

def get_namespace(element):
    match = None
    if element.tag.startswith('{'):
        match = re.match(r'\{.*?\}', element.tag)
    return match.group(0) if match else ''

def get_namespace_no_tag(root: str):
    pattern = r"\{(.*?)\}"
    return re.match(pattern, root).group(0)

def filterXMLAndTurnToCSV(directory: str):
    pattern = r"(?<=/)([^/]+)(?=/__text)"

    xml_files = os.listdir(directory)
    for xml_file in xml_files:
        with open(xml_file, 'r') as file:
            reader = csv.reader

    # Input CSV file and new header values
    input_file = 'input.csv'
    output_file = 'output.csv'
    new_headers = ['new_coordinates', 'new_crown_width']

    # Define the regular expression pattern to extract the values
    pattern = r"(?<=/)([^/]+)(?=/__text)"

    # Read the input CSV file
    with open(input_file, 'r') as file:
        reader = csv.reader(file)
        rows = list(reader)

    # Modify the header row and delete columns with unmatched headers
    header_row = rows[0]
    column_indices_to_delete = []
    for i, header in enumerate(header_row):
        match = re.search(pattern, header)
        if match:
            new_header = new_headers[i]
            header_row[i] = new_header
        else:
            column_indices_to_delete.append(i)

    # Delete columns with unmatched headers from the data rows
    for row in rows:
        for index in sorted(column_indices_to_delete, reverse=True):
            del row[index]

    # Write the updated data to the output CSV file
    with open(output_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(rows)

    print("CSV headers updated and columns deleted successfully.")
