const axios = require('axios');
const { parseString } = require('xml2js');
const fs = require('fs');
const path = require('path');

// required to merge XML's
const { create } = require('xmlbuilder2');


// only for development purposes. Please delete if/when creating a pull request for production (mainline).
const wfs_url_that_does_not_support_paging = "https://geodienste.halle.de/opendata/fa3930b7-b3ed-b3fc-20d9-2fc8fd054b0e?service=WFS&version=1.1.0&request=GetFeature&typeName=fa3930b7-b3ed-b3fc-20d9-2fc8fd054b0e&srsName=EPSG:4326";
const wfs_url_that_accepts_a_higher_version = "https://gis.gouda.nl/geoserver/BOR/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=BOR:V_BOMEN_ALLES&srsName=EPSG:4326";
const wfs_url_with_less_than_10_000_results = "https://cugir.library.cornell.edu/geoserver/cugir/wfs?service=WFS&version=2.0.0&request=GetFeature&typeName=cugir009100&srsName=EPSG:4326";
const wfs_url_with_less_than_5_000_results = "https://geoservices-others.irisnet.be/geoserver/UrbisAasArbre/ows?service=WFS&version=2.0.0&request=GetFeature&typeNames=UrbisAasArbre:arbres_jette_vue&srsName=EPSG:4326"
const wfs_url_with_more_than_100_000_results = "https://maps.groningen.nl/geoserver/geo-data/ows?service=WFS&version=2.0.0&request=GetFeature&typeName=geo-data:Bomen+gemeente+Groningen&srsName=EPSG:4326"
const wfs_url_with_less_than_1000_results = "https://geoportale.regione.lazio.it/geoserver/ows?service=WFS&version=2.0.0&request=GetFeature&typeNames=geonode:alberi_monumentali_20190624&srsName=EPSG:4326";
const wfs_url_test_num_5 = "https://maps.groningen.nl/geoserver/geo-data/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=geo-data:Bomen+gemeente+Groningen&srsName=EPSG:4326";


/**
 * Take a GetFeatures url and turn it into a GetCapabilities url.
 * 
 * @param {string} featuresUrl WFS GetFeatures url in which we want to download data from. We will turn this into a GetCapabilities url.
 * @returns {string}
 */
function transformFeaturesUrlToCapabilitiesUrl(featuresUrl) {
    return featuresUrl.replace('request=GetFeature', 'request=GetCapabilities')
}

/**
 * The regular expression /&version=\d+\.\d+\.\d+/ matches the "version=#.#.0" pattern, where # represents any digit. The replace() method replaces this pattern with an empty string, effectively removing it from the URL.
 * This is so we can ensure that we are working with the highest available WFS version.
 * 
 * @param {string} capabilitiesUrl WFS GetCapabilities Url
 * @returns {string}
 */
function removeVersionFromUrl(capabilitiesUrl) {
    return capabilitiesUrl.replace(/&version=\d+\.\d+\.\d+/, '');
}


/**
 * Make a request to retrieve the XML data from the GetCapabilities request. Then utilize the fast-xml-parser library to turn this XML data into a JSON object.
 * @param {string} wfsUrl WFS GetCapabilities Url with version removed.
 * @returns {string}
 */
async function retrieveXMLDataFromWFSUrl(wfsUrl) {
    try {
        const response = await axios.get(wfsUrl);
        return response
    } catch (error) {
        console.log("error during get request: ", wfsUrl)
        throw error;
    }
}


/**
 * When getting the accepted versions from a WFS url, it will return either a list (or an array in javascript) or just the standalone object.
 * If its an array, grab the last element in the array (the last element represents the highest version) or just return the value straight up if its not an array.
 * @param {*} acceptedVersions
 * @returns 
 */
function getLastElement(acceptedVersions) {
    if (Array.isArray(acceptedVersions)) {
      return acceptedVersions[acceptedVersions.length - 1];
    } else {
      return acceptedVersions;
    }
}


/**
 * Leverage the xml2js library to turn the XML into a JSON object. Must be a promise so we don't try to use
 * the returned data before its done being parsed.
 * 
 * @param {*} capabilitiesXML 
 * @returns {JSON}
 */
function convertGetCapabilitiesXMLToJson(capabilitiesXML) {
    return new Promise((resolve, reject) => {
        parseString(capabilitiesXML, (error, result) => {
          if (error) {
            reject(error);
            return;
          }
          
          resolve(result);
        });
      });
}


/**  
 * Parsing the JSON we have and finding out if we can page or not by looking for the 'ImplementResultPaging'
 * 
 * @param {JSON} getCapabilitiesJSON 
 */
function parseGetCapabilitiesJsonForPagingInformation(getCapabilitiesJSON) {
    var possibleBeginningKeys = ['WFS_Capabilities', 'wfs:WFS_Capabilities'];

    for (key of possibleBeginningKeys) {
        if (typeof getCapabilitiesJSON[key] !== 'undefined') {
            var operationsMetadata = getCapabilitiesJSON[key]['ows:OperationsMetadata'][0];
            var constraintsList = operationsMetadata['ows:Constraint'];
            for (constraint of constraintsList) {
                if (constraint['$'].name === 'ImplementsResultPaging') {
                    return constraint['ows:DefaultValue'][0];
                }
            }
            return 0;
        } else {
            console.log("working..");
        }
    }

}


/**
 * Parse through the XML that was turned into a dictionary and find the highest WFS version that we can use.
 * The value in the dictionary could end up being a list or a standalone value, such as:
 * 1. {"versions": "2.0.0"}
 * 2. {"versions: ["1.1.0", "2.0.0"]}
 * 
 * @param {*} getCapabilitiesJSON JSON object containing values from the GetCapabilities XML.
 * @returns Highest version the WFS accepts for that particular URL.
 */
function parseGetCapabilitiesJsonForHighestVersion(getCapabilitiesJSON) {
    acceptVersions = []
    var possibleBeginningKeys = ['WFS_Capabilities', 'wfs:WFS_Capabilities'];

    for (key of possibleBeginningKeys) {
        if (typeof getCapabilitiesJSON[key] !== 'undefined') {
            var operationsList = getCapabilitiesJSON[key]['ows:OperationsMetadata'][0]['ows:Operation'];
            for (operation of operationsList) {
                if (operation['$'].name === "GetCapabilities") {
                    parametersList = operation["ows:Parameter"]
                    for (parameter of parametersList) {
                        if (parameter['$'].name == "AcceptVersions") {
                            return getLastElement(parameter['ows:AllowedValues'][0]['ows:Value']); 
                        }
                    }
                }
            }
            return 0;
        } else {
            console.log("working.");
        }
    }
}


/**
 * Parse through the XML that was turned into a JSON object and find if we can do &resultType=hits query at the end of a GetFeatures URL.
 * 
 * @param {*} getCapabilitiesJSON 
 * @returns 
 */
function parseGetCapabilitiesJsonForResultType(getCapabilitiesJSON) {
    acceptVersions = []
    var possibleBeginningKeys = ['WFS_Capabilities', 'wfs:WFS_Capabilities'];

    for (key of possibleBeginningKeys) {
        if (typeof getCapabilitiesJSON[key] !== 'undefined') {
            var operationsList = getCapabilitiesJSON[key]['ows:OperationsMetadata'][0]['ows:Operation'];
            for (operation of operationsList) {
                if (operation['$'].name === "GetFeature") {
                    parametersList = operation["ows:Parameter"]
                    for (parameter of parametersList) {
                        if (parameter['$'].name == "resultType") {
                            return getLastElement(parameter['ows:AllowedValues'][0]['ows:Value']); 
                        }
                    }
                }
            }
            return 0;
        } else {
            console.log("working...");
        }
    }
}


/**
 * @todo: (QOL) Consider doing this async earlier in the program...
 * 
 * Creates the directory that we store the XML data in..
 * @returns 
 */
function prepareDataDownloadsDirectory() {
    /**
     * Consider creating this async since we don't need this right away...
     */
    var directoryName = path.join(process.cwd(), 'data_downloads');

    if (!fs.existsSync(directoryName)) {
        fs.mkdirSync(directoryName, {recursive: true});
    }
    return directoryName;
}


/** 
 * When/if we can page, the XML arguments will contain a 'next' and/or a 'previous' token. If there is a 'next' token, then that means we can continue paging. Otherwise, its likely that we do not need
 * to continue paging.
 * @param {string} xmlString 
 * @returns {boolean} boolean indicating if we should continue paging or not
 */
function shouldContinuePaging(xmlString) {
    jsonObj = convertGetCapabilitiesXMLToJson(xmlString);
    return jsonObj.then(jsonData => {
        if (jsonData['wfs:FeatureCollection']['$'].next) {
            return true;
          } else {
            return false;
          }
    });
}


/**
 * 
 * Page through the WFS results and save the XMLs to the /data_downloads directory.
 * 
 * @param {string} getFeatureUrl 
 * @param {string} highestVersion
 */
async function pageThroughResults(getFeatureUrl, highestVersion) {
    var max_results_per_page = 20; // FEEL FREE TO CHANGE THIS IF USING A WFS URL LINK THAT HAS MORE THAN 1500 ITEMS. VIEW THE AVAILABLE WFS URLS AT THE TOP OF THIS FILE.
    var startIndex = 0;
    var statusCode = 200;
    var iterations = 0;
    var xmlFiles = [];
    var nextToken = true;
    var shouldContinue = true;
    try {
        // create the directory that will hold the XMLs.. will only create if it doesnt exist
        var directoryName = prepareDataDownloadsDirectory();
        while (shouldContinue) {

            var pagingUrl = getFeatureUrl +  `&maxFeatures=${max_results_per_page}&startIndex=${startIndex}`;
            if (highestVersion === '2.0.0') {
                pagingUrl = getFeatureUrl + `&count=${max_results_per_page}&startIndex=${startIndex}`;
            }
            
            // grabbing the XML content from the GetFeature request.
            featureData = await retrieveXMLDataFromWFSUrl(pagingUrl);
            statusCode = featureData.status;

            if (statusCode !== 200) {
                shouldContinue = false;
            }

            // first check if nextToken is available. If the next token exists then that means we can page and should continue.
            await shouldContinuePaging(featureData.data).then(result => {
                nextToken = result;
            });

            if (nextToken == false) {
                shouldContinue = false;
            }

            xmlFileName = `dataset${iterations}.xml`;
            xmlFilePath = path.join(directoryName, xmlFileName);

            xmlFiles.push(xmlFilePath);

            // creating an XML file in the /data_downloads directory with the content from the first iteration of the GetFeature request.
            fs.writeFileSync(xmlFilePath, featureData.data, 'binary');

            iterations = iterations + 1;
            startIndex = startIndex + max_results_per_page
        }
    } catch (error) {
        console.error('Error during pageThroughResults: ', error.message);
        throw error;
    }
    return xmlFiles;
}


/**
 * We check if we get query for number if results. If we can query for results and the number of results is less than 100,000 then we will download the dataset. Otherwise, we will not download the dataset 
 * as it will be too much and possibly cause issues with memory / server timeouts.
 * @param {string} getFeatureUrl 
 * @param {string} highestVersion 
 */
async function getResultsOnlyIfLessThan100k(getFeatureUrl, highestVersion) {
    try {
        var hitsUrl = getFeatureUrl +  `&resultType=hits` + `&version=${highestVersion}`;
        featureData = await retrieveXMLDataFromWFSUrl(hitsUrl);
        statusCode = featureData.status;

        if (statusCode !== 200) {
            console.log('the call to ' + hitsUrl + ' failed. Exiting...');
            exit();
        }

        var hitsResults = await convertGetCapabilitiesXMLToJson(featureData.data);

        const firstKey = Object.keys(hitsResults)[0];
        const numberMatched = hitsResults[firstKey]['$']['numberMatched'];
        if (parseInt(numberMatched) < 100_000) {
            console.log("Downloading content...");
            var directoryName = prepareDataDownloadsDirectory();

            featureData = await retrieveXMLDataFromWFSUrl(getFeatureUrl);

            xmlFileName = `dataset.xml`;
            xmlFilePath = path.join(directoryName, xmlFileName);

            fs.writeFileSync(xmlFilePath, featureData.data, 'binary');
            return;

        }
        console.log("The url does not support paging and has too many results to get in one call. Aborting program...");
        exit();


    } catch (error) {
        console.log("There was en error attempting to retrieve data... please try again with a different url.");
        exit();
    }
}


/**
 * @param {*} xmlFiles - List/Array of paths of XML files we downloaded.
 * Function to merge together any XML files we have in the /data_downloads directory
 */
function mergeXMLFiles(xmlFiles) {
    console.log("Almost done messing with the data...");

    const mergedXml = create().ele('root');

    xmlFiles.forEach(xmlFile => {
        // Read the XML content from the file
        const xmlContent = fs.readFileSync(xmlFile, 'utf8');
      
        // Parse the XML content into a JavaScript object
        const xmlDoc = create(xmlContent);

        // Get the root element of the parsed XML document
        const rootElement = xmlDoc.root();

        // Import the child nodes of the root element into the merged XML document
        rootElement.each(childNode => {
            mergedXml.import(childNode);
        });
    });

    // Convert the merged JavaScript object back to XML
    const mergedXmlString = mergedXml.end({ prettyPrint: true });
  
    // Save the merged XML to a local directory
    fs.writeFileSync('merged.xml', mergedXmlString, 'utf8');
}


/**
 * Loop through the XML files we downloaded and delete them after we've merged them and no longer need them.
 * @param {*} xmlFiles List/Array of paths of XML files we downloaded.
 */
function deleteLeftOverXMLFiles(xmlFiles) {
    console.log("Cleaning up some of our mess...");
    for (file of xmlFiles) {
        fs.unlink(file, (err) => {
            if (err) {
                console.error("error deleting file: ", file);
                return;
            }
        })
    }
}


/**
 * Blackbox function that calls various methods to parse a WFS url and return information necessary to download objects, if possible.
 * @param {string} wfsUrl WFS GetFeatures url in which we want to download data from.
 */
async function downloadWfsDataInXmlFormat(wfsUrl) {

    // In the url it looks like "&version=#.#.0". Removing the version will redirect us to the highest version that the server accepts.
    // So just in case we receive a link that has version 1.0.0, we can make sure that a higher version doesn't exist.
    var urlWithoutVersion = removeVersionFromUrl(wfsUrl);

    // First, we change the url to be a GetCapabilities Request. As mentioned in the TODO, we can likely skip these next few methods and just go straight to paging (via the pageThroughResults method).
    var getCapabilitiesUrl = transformFeaturesUrlToCapabilitiesUrl(urlWithoutVersion);

    console.log("Retrieving capabilities...");

    // We get the XML content from the GetCapabilities request
    var getCapabilitiesXML = await retrieveXMLDataFromWFSUrl(getCapabilitiesUrl)
    var capabilitiesData = getCapabilitiesXML.data

    console.log("Doing some magic...");
    // Turn the GetCapabilities XML to a JSON object so its easier to work with.
    var getCapabilitiesJson = await convertGetCapabilitiesXMLToJson(capabilitiesData)

    var highestVersion = parseGetCapabilitiesJsonForHighestVersion(getCapabilitiesJson);

    if (highestVersion === 0) {
        console.log("The WFS server has not declared the versions it accepts");
        exit();
    }

    // Based on the JSON, we check to see if the server allows for paging.
    var canPage = parseGetCapabilitiesJsonForPagingInformation(getCapabilitiesJson);

    var featuresUrlWithHighestVersion = urlWithoutVersion + `&version=${highestVersion}`

    // If we can't page, then lets check to see if we can at least get all the content without needing to worry about paging anyway.
    var xmlFiles = [];

    if (canPage === 0) {
        console.log("The server has not delcared paging as an option... checking to see if we can download all content...");
        var resultType = parseGetCapabilitiesJsonForResultType(getCapabilitiesJson);
        if (resultType === 'hits') {
            getResultsOnlyIfLessThan100k(featuresUrlWithHighestVersion, highestVersion);
        }
    } else {
        console.log("This server contains a lot of content, beginning to page through it...");
        xmlFiles = await pageThroughResults(featuresUrlWithHighestVersion, highestVersion);
    }

    // after paging, we should merge any xml files we have.
    mergeXMLFiles(xmlFiles)
    deleteLeftOverXMLFiles(xmlFiles);
    console.log("Data has been downloaded to the /data_downloads directory.");
}

/**
 * @todo: (REQUIRED) test more using the wfs link at the top of this file.
 */
downloadWfsDataInXmlFormat(wfs_url_with_less_than_1000_results)