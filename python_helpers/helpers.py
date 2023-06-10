import sys
import requests
from xml.etree.ElementTree import Element, fromstring, ElementTree
from urllib.parse import urlparse, urlunparse, parse_qs
import xmltodict
import os
from csv_helpers import xml_to_csv_part_5


WFS_URL1 = "https://cugir.library.cornell.edu/geoserver/cugir/wfs?service=WFS&version=2.0.0&request=GetFeature&typeName=cugir009097&srsName=EPSG:4326"
WFS_URL = "https://geo.sv.rostock.de/inspire/lcv-trees/download?service=WFS&version=2.0.0&request=GetFeature&typeNames=lcv:LandCoverUnit&srsName=EPSG:4326"
WFS_URL2 = "https://services.nijmegen.nl/geoservices/extern_BOR_Groen/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=extern_BOR_Groen:GRN_BOMEN&srsName=EPSG:4326"
WFS_URL3 = "https://data.gov.au/geoserver/tree-register/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=ckan_13079796_1b3a_448d_8a09_d7fda5611c5f&srsName=EPSG:4326"
MAX_RESULTS = 100_000


############################################################################ START --- FUNCTIONS TO HELP DEBUG / TEST -----  ##################################################

def getCWDForTesting():
    return os.getcwd() + '/request_context.txt'


def writeXMLToTextFileForTesting(xml_string: str):
    file1 = open(getCWDForTesting(), 'w')
    print("writing to file...")
    file1.write(bytesToString(xml_string))
    print("closing file...")
    file1.close()


def writeParsedXMLToFile(parsed_xml: str):
    file1 = open(getCWDForTesting(), 'w')
    print("writing to file...")
    file1.write(parsed_xml)
    print("closing file...")
    file1.close()

############################################################################ END --- FUNCTIONS TO HELP DEBUG / TEST -----  ##################################################

def bytesToString(bytes_data: bytes) -> str:
    """
    The requests.get() function returns bytes. We should turn these into a proper string.
    Args:
        bytes_data: series of bytes to decode

    Return:
        string from bytes
    """
    return bytes_data.decode('utf-8')

def shouldBeLastXMLDownload(xml_bytes: str, max_results: int) -> bool:
    """
    @TODO: Clean up print functions
    We check if the XML passed in is technically the last XML we need to download. More explanation can be found in the paging function.

    Args:
        xml_bytes: bytes to turn into string representing the xml we retrieved from wfs url.
        max_results: maximum number of results we get per paging transaction.
    
    Return:
        bool representing if we should keep downloading or not.
    """
    content = bytesToString(xml_bytes)
    root = fromstring(content)
    number_returned = root.attrib['numberReturned']
    print("numnber returned: ", number_returned)
    if int(number_returned) < max_results:
        return True
    return False
    

def transformFeaturesUrlForCapabilities(featuresUrl: str) -> str:
    return featuresUrl.replace('request=GetFeature', 'request=GetCapabilities')


def removeVersionFromUrl(wfsUrl: str) -> str:
    """
    Takes a raw url that points to a WFS service and returns it without the "Version=#.#.0" parameter. This is so we can ensure that we are working
    with the highest available WFS version.

    Args:
        wfsUrl: String indicating raw UFS url
    
    Returns:
        WFS url without "version=#.#.0" in the url.
    """

    parsed_url = urlparse(wfsUrl)
    query_params = parse_qs(parsed_url.query)

    # Remove the "version" parameter
    query_params.pop('version', None)

    # Update the query string without the "version" parameter
    new_query_string = '&'.join(f"{key}={value[0]}" for key, value in query_params.items())

    # Create the updated URL
    updated_url = urlunparse((
        parsed_url.scheme,
        parsed_url.netloc,
        parsed_url.path,
        parsed_url.params,
        new_query_string,
        parsed_url.fragment
    ))

    return updated_url

def parseXML(capabilitiesString: str) -> Element:
    """
    Take the webpage represented as a string that was retrieved from the requests operation and turn it into XML.

    Args:
        capabilitiesString: string representing the content from the capabilities webpage.
    
    Returns:
        XML.etree.ElementTree.Element object containing content from the capabilities webpage.
    """

    capabilitiesXML = fromstring(capabilitiesString)
    return capabilitiesXML


def getResultTypeFromXMLDict(xml_dict):
    """
    Parse through the XML that was turned into a dictionary and find if we can do &resultType=hits query at the end of a GetFeatures URL.

    @TODO: Just return the entire list and then we can check if hits is in list.

    Args:
        xml_dict: Dictionary containing values from the GetCapabilities XML.

    Return:
        Highest string indicating if we can perform paging operations.
    """
    possibleBeginningKeys = ['WFS_Capabilities', 'wfs:WFS_Capabilities']
    resultTypes = None
    for key in possibleBeginningKeys:
        if key in xml_dict:
            operationList = xml_dict[key]["ows:OperationsMetadata"]["ows:Operation"]
            for operation in operationList:
                if operation["@name"] == "GetFeature":
                    parametersList = operation["ows:Parameter"]
                    for parameter in parametersList:
                        if parameter["@name"] == "resultType":
                            resultTypes = parameter["ows:AllowedValues"]["ows:Value"]
                            print(resultTypes)
    if type(resultTypes) == list:
        return resultTypes[-1]
    else:
        return resultTypes
    

def getImplementsPagingResultFromXMLDict(xml_dict):
    """
    Parse through the XML that was turned into a dictionary and find if the server implements paging. In XML it will look like such:
    <ows:Constraint name="ImplementsResultPaging">
        <ows:NoValues/>
        <ows:DefaultValue>TRUE</ows:DefaultValue>
    <ows:Constraint />

    Args:
        xml_dict: Dictionary containing values from the GetCapabilities XML.

    Return:
        Highest string indicating if we can perform paging operations.
    """
    possibleBeginningKeys = ['WFS_Capabilities', 'wfs:WFS_Capabilities']
    implementsPagingResult = None
    for key in possibleBeginningKeys:
        if key in xml_dict:
            constraintsList = xml_dict[key]["ows:OperationsMetadata"]["ows:Constraint"]
            for constraint in constraintsList:
                if constraint["@name"] == "ImplementsResultPaging":
                    print(constraint)
                    return constraint["ows:DefaultValue"]
            return implementsPagingResult


def getHighestVersionFromXMLDict(parsed_xml):
    """
    Parse through the XML that was turned into a dictionary and find the highest WFS version that we can use.
    The value in the dictionary could end up being a list or a standalone value, such as:
    1. {"versions": "2.0.0"}
    2. {"versions: ["1.1.0", "2.0.0"]}
    The if statement at the end accounts for this possibility.

    Args:
        xml_dict: Dictionary containing values from the GetCapabilities XML.

    Return:
        Highest version the WFS accepts for that particular URL.
    """
    possibleBeginningKeys = ['WFS_Capabilities', 'wfs:WFS_Capabilities']
    accept_versions = None
    for key in possibleBeginningKeys:
        if key in parsed_xml:
            operationList = parsed_xml[key]["ows:OperationsMetadata"]["ows:Operation"]
            for operation in operationList:
                if operation["@name"] == "GetCapabilities":
                    parametersList = operation["ows:Parameter"]
                    for parameter in parametersList:
                        if parameter["@name"] == "AcceptVersions":
                            accept_versions = parameter["ows:AllowedValues"]["ows:Value"]
                            print(accept_versions)
    
    if type(accept_versions) == list:
        return accept_versions[-1]
    else:
        return accept_versions
    

def pageThroughResults(url_version: str, wfs_url: str):
    """
    @TODO: Clean up
    @TODO: Implement 1.0.0 and 1.1.0 functionality
    @TODO: Move the directory creation elsewhere so we're not calling it everytime, will save on latency

    If a WFS server allows paging then we will page through the results. 100,000 at a time. Paging works by utilizing the startIndex parameter.
    WFS urls are paged like url.com/.../...&count=200&startIndex=200 -> url /...&count=200&startIndex=400.. 
    so the startIndex is not single digit sequential aka not 0, 1, 2, 3 etc (at least it shouldn't due to the way we're needing the data).

    Downloaded files will be in the format of dataset#.xml where # is an integer from 0-9
    We will download the XMLs to a local directory for easier access.

    Args:
        url_version: should be the highest version available. Acceptable value should be 1.0.0, 1.1.0, 2.0.0
        wfs_url: url in which we will be downloading the data from.

    Return:
        Nothing. Datasets will be downloaded to local disk.
    """
    print("version: " + url_version)
    max_results_per_page = 200 # temporary value for deveopment testing. We should use the global variable above instead.
    iterations = 0
    startIndex = 0
    statusCode = 200
    download_file_name = None
    directory_name = None

    if url_version == "2.0.0":
        while (statusCode == 200 and iterations < 10):
            pagingUrl = wfs_url + f"&count={max_results_per_page}&startIndex={startIndex}"
            print("index: ", startIndex)
            print("url: ", pagingUrl)
            r = requests.get(pagingUrl)

            # status code lets us know if the request was successful or not. a 200 status code indicates a successful request. Anything else is likely a failure and we should abort.
            statusCode = r.status_code
            print("status code: ", statusCode)


            # All these lines are doing are just getting directory names, file names, and creating directories if necessary.
            download_file_name = "dataset"+str(iterations)+".xml"
            directory_name = os.path.join(os.getcwd(), 'data_downloads')
            print("directory name: " + directory_name)

            if not os.path.exists(directory_name):
                print("creating directory...")
                os.makedirs(directory_name)

            download_file_name = os.path.join(directory_name, download_file_name)
            print("downloading file: " + download_file_name)

            # next we check if the xml we are currently on is the last one we need to download. We know if its the last one if the parameter/header in the XML file contains numberReturned = ###. the "###" in this case will be a number that should be less than our
            # max results per page. If the number is less than max_results_per_page, we know we're done paging and don't need to make another request, so we break.
            if shouldBeLastXMLDownload(r.content, max_results_per_page):
                open(download_file_name, 'wb').write(r.content)
                print("done downloading data...")
                break
            else:
                open(download_file_name, 'wb').write(r.content)
                startIndex = startIndex + max_results_per_page
                iterations = iterations + 1
    # Complete this portion. Needs to be the same as above...
    else:
        while (statusCode == 200 and iterations < 10):
            wfs_url = wfs_url + f"&maxFeatures={max_results_per_page}&startIndex={startIndex}"
            print("index: ", startIndex)
            r = requests.get(wfs_url)
            statusCode = r.status_code
            print("status code: ", statusCode)
            download_file_name = "dataset"+str(startIndex)+".xml"
            print("downloading file: " + download_file_name)
            open(download_file_name, 'wb').write(r.content)
            print("downloaded datasets.")
            startIndex = startIndex + max_results_per_page
            iterations = iterations + 1
    
    xml_to_csv_part_5(directory_name)
    

        
def getWFSXML():
    """
    Blackbox function that calls various methods to parse a WFS url and return information necessary to download objects, if possible.

    @TODO: Currently we just pass in the URL from the CLI to the page results function. We need to ensure we're using the highest version available either by removing the version or plugging in the version we
    get from getHighestVersionFromXMLDict
    @TODO: Rename this function to something else like downloadWFSData(). To make it more clear
    @TODO: Handle when we cannot page
    @TODO: consider retrieving outputFormat allowedValues from a WFS url GetCapabilities request. This would let us know if the server already serves a CSV response and would remove the need for us to do it ourself most of the time (unless XML is fine of course).
           if it serves a CSV response then we can ask the server to give us that instead of the XML.
    """
    print("retrieved following args: ", sys.argv[1])


    wfs_capabilities_url = transformFeaturesUrlForCapabilities(sys.argv[1])
    print("getting content from url: ", wfs_capabilities_url)

    urlWithoutVersion = removeVersionFromUrl(wfs_capabilities_url) + ".xml"
    print("getting request...", urlWithoutVersion)

    r = requests.get(urlWithoutVersion)
    parsed_xml = xmltodict.parse(r.content)
    print("xml has been parsed...")

    version = getHighestVersionFromXMLDict(parsed_xml)
    canPage = getImplementsPagingResultFromXMLDict(parsed_xml)

    print(canPage)
    if canPage == "TRUE":
        pageThroughResults(version, removeVersionFromUrl(sys.argv[1]))
    else:
        print("cannot page. not downloading...")


if __name__ == "__main__":
    print("hello world")
    getWFSXML()