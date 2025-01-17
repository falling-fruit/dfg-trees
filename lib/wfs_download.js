const axios = require('axios');
const { parseString } = require('xml2js');
const fs = require('fs');
const path = require('path');

// required to merge XML's
const { create } = require('xmlbuilder2');


/**
 * Take a GetFeatures url and turn it into a GetCapabilities url.
 * 
 * @param {string} featuresUrl WFS GetFeatures url in which we want to download data from. We will turn this into a GetCapabilities url.
 * @returns {string}
 */
function transformFeaturesUrlToCapabilitiesUrl(featuresUrl) {
    return featuresUrl.replace('request=GetFeature', 'request=GetCapabilities');
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
 * Make a request to retrieve the XML data from the GetCapabilities request.
 * @param {string} wfsUrl WFS GetCapabilities Url with version removed.
 * @returns {string} XML in the form of a string
 */
async function retrieveXMLDataFromWFSUrl(wfsUrl) {
    try {
        const response = await axios.get(wfsUrl);
        return response;
    } catch (error) {
        console.log("error during get request: ", wfsUrl)
        throw error.message;
    }
}


/**
 * When getting the accepted versions from a WFS url, it will return either a list (or an array in javascript) or just the standalone object.
 * If its an array, grab the highest version available or just return the value straight up if its not an array.
 * @param {string[]|string} acceptedVersions
 * @returns {string} the highest version the server accepts.
 */
function getHighestVersionFromAcceptedVersionsParameter(acceptedVersions) {
    const highestToLowestVersions = ['2.0.0', '1.1.0', '1.0.0'];
    if (Array.isArray(acceptedVersions)) {
        for (var version of highestToLowestVersions) {
            if (acceptedVersions.some(item => item.trim() === version.trim())) {
                return version;
            }
        }
    } else {
        return acceptedVersions;
    }
}


/**
 * Self explanatory function, we should receive either a standalone string or an array of strings. one of these values should indicate if we can query
 * GetFeature with resultType=hits or not.
 * @param {string[]|string} resultType 
 * @returns {boolean} bool indicating if resultType = hits is a valid query.
 */
function doesResultTypeParameterIncludeHits(resultType) {
    if (Array.isArray(resultType)) {
        if(resultType.some(item => item.trim() === 'hits')) {
            return true;
        }
        return false;
    } else {
        return resultType.trim() === 'hits';
    }
}


/**
 * Leverage the xml2js library to turn the XML into a JSON object. Must be a promise so we don't try to use
 * the returned data before its done being parsed.
 * 
 * @param {string} capabilitiesXML XML that represents the data from a GetCapabilities request
 * @returns {object} the JSON object we converted from the passed in XML;
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
 * @param {object} getCapabilitiesJSON JSON object containing values from the GetCapabilities XML.
 * @returns {string} string indicating if the server allows paging or not, in the form of "TRUE" or "FALSE"
 */
function parseGetCapabilitiesJsonForPagingInformation(getCapabilitiesJSON) {
    const possibleBeginningKeys = ['WFS_Capabilities', 'wfs:WFS_Capabilities'];

    for (var key of possibleBeginningKeys) {
        if (typeof getCapabilitiesJSON[key] !== 'undefined') {
            var operationsMetadata = getCapabilitiesJSON[key]['ows:OperationsMetadata'][0];
            if (operationsMetadata['ows:Constraint'] !== undefined) {
                var constraintsList = operationsMetadata['ows:Constraint'];
                for (var constraint of constraintsList) {
                    if (constraint['$'].name === 'ImplementsResultPaging') {
                        if (constraint['ows:DefaultValue'][0] === 'FALSE') {
                            return false;
                        }
                        return true;
                    }
                }
                return false;
            }
            return false;
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
 * @param {object} getCapabilitiesJSON JSON object containing values from the GetCapabilities XML.
 * @returns {string[]|string} Highest version the WFS accepts for that particular URL.
 */
function parseGetCapabilitiesJsonForHighestVersion(getCapabilitiesJSON) {
    const possibleBeginningKeys = ['WFS_Capabilities', 'wfs:WFS_Capabilities'];

    for (var key of possibleBeginningKeys) {
        if (typeof getCapabilitiesJSON[key] !== 'undefined') {
            var operationsList = getCapabilitiesJSON[key]['ows:OperationsMetadata'][0]['ows:Operation'];
            for (var operation of operationsList) {
                if (operation['$'].name === "GetCapabilities") {
                    parametersList = operation["ows:Parameter"]
                    for (var parameter of parametersList) {
                        if (parameter['$'].name == "AcceptVersions") {
                            if (parameter['ows:AllowedValues'] !== undefined) {
                                return getHighestVersionFromAcceptedVersionsParameter(parameter['ows:AllowedValues'][0]['ows:Value'])
                            }

                            if (parameter['ows:Value'] !== undefined) {
                                return getHighestVersionFromAcceptedVersionsParameter(parameter['ows:Value']); 
                            }
                        }
                    }
                }
            }
            return false;
        } else {
            console.log("working.");
        }
    }
}


/**
 * Parse through the JSON and find if we can do &resultType=hits query at the end of a GetFeatures URL.
 * 
 * @param {object} getCapabilitiesJSON 
 * @returns {boolean} indicating if we can use resultType=hits query in the wfs url parameter.
 */
function parseGetCapabilitiesJsonForResultType(getCapabilitiesJSON) {
    const possibleBeginningKeys = ['WFS_Capabilities', 'wfs:WFS_Capabilities'];

    for (var key of possibleBeginningKeys) {
        if (typeof getCapabilitiesJSON[key] !== 'undefined') {
            const operationsList = getCapabilitiesJSON[key]['ows:OperationsMetadata'][0]['ows:Operation'];
            for (var operation of operationsList) {
                if (operation['$'].name === "GetFeature") {
                    if (operation["ows:Parameter"]) {
                        const parametersList = operation["ows:Parameter"];
                        for (var parameter of parametersList) {
                            if (parameter['$'].name == "resultType") {
                                if (parameter['ows:AllowedValues']) {
                                    return doesResultTypeParameterIncludeHits(parameter['ows:AllowedValues'][0]['ows:Value']); 
                                }
                                return doesResultTypeParameterIncludeHits(parameter['ows:Value']); 
                            }
                        }
                    } else {
                        return false;
                    }
                }
            }
            return false;
        } else {
            console.log("working...");
        }
    }
}


/** 
 * Creates the directory that we store the XML data in.
 * @param {string} outputDirectoryName the desired output directory name
 * @returns {string} representing full path in which to save the XML files.
 */
function prepareDataDownloadsDirectory(outputDirectoryName) {
    /**
     * Consider creating this async since we don't need this right away...
     */
    const directoryName = path.join(process.cwd(), outputDirectoryName);

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
async function shouldContinuePaging(xmlString) {
    var jsonObj = await convertGetCapabilitiesXMLToJson(xmlString);
    if (jsonObj) {
        if (jsonObj['wfs:FeatureCollection']) {
            if (jsonObj['wfs:FeatureCollection']['$'].next) {
                return true;
            }
            return false;
        } else {
            // There has been occurences where a server has not setup their database properly and we actually cannot page for items.
            throw new Error("There is an error when retrieving the paging information. Likely a java.io.IOException on the server side. Aborting the program");
        }
    }
}


/**
 * 
 * Page through the WFS results and save the XMLs to the /data_downloads directory.
 * 
 * @param {string} getFeatureUrl the URL that represents a WFS GetFeature request
 * @param {string} highestVersion the highest version the server accepts, should be '1.0.0', '1.1.0', or '2.0.0'.
 */
async function pageThroughResults(getFeatureUrl, highestVersion, outputDirectoryName) {
    const max_results_per_page = 50000;
    var startIndex = 0;
    const statusCode = 200;
    var iterations = 0;
    var xmlFiles = [];
    var shouldContinue = true;
    try {
        if (highestVersion === '2.0.0') {
            getFeatureUrl = getFeatureUrl.replace('typeName=', 'typeNames=');
        }
        // create the directory that will hold the XMLs.. will only create if it doesnt exist
        var directoryName = prepareDataDownloadsDirectory(outputDirectoryName);
        while (shouldContinue) {

            var pagingUrl = getFeatureUrl +  `&maxFeatures=${max_results_per_page}&startIndex=${startIndex}`;
            if (highestVersion === '2.0.0') {
                pagingUrl = getFeatureUrl + `&count=${max_results_per_page}&startIndex=${startIndex}`;
            }

            // grabbing the XML content from the GetFeature request.
            featureData = await retrieveXMLDataFromWFSUrl(pagingUrl);
            if (featureData.status) {
                statusCode = featureData.status;
            }

            if (statusCode !== 200 || featureData === undefined) {
                shouldContinue = false;
            }

            // first check if nextToken is available. If the next token exists then that means we can page and should continue.
            shouldContinue = await shouldContinuePaging(featureData.data);

            xmlFileName = `dataset${iterations}.xml`;
            xmlFilePath = path.join(directoryName, xmlFileName);

            xmlFiles.push(xmlFilePath);

            // creating an XML file in the /data_downloads directory with the content from the first iteration of the GetFeature request.
            fs.writeFileSync(xmlFilePath, featureData.data, 'binary');

            iterations = iterations + 1;
            startIndex = startIndex + max_results_per_page;
        }
    } catch (error) {
        throw error.message;
    }
    return xmlFiles;
}


/**
 * If we can query for number of results and the number of results is less than 100,000 then we will download the dataset straight up. Otherwise, we will not download the dataset 
 * as it will be too much and possibly cause issues with memory / server timeouts and thus will move on to see if we can page instead.
 * @param {string} getFeatureUrl wfs url that represents a GetFeature request
 * @param {string} highestVersion the highest version the wfs server accepts for this particular url should be one of '1.0.0', '1.1.0', or '2.0.0'
 */
async function getResultsOnlyIfLessThan100k(getFeatureUrl, highestVersion, outputDirectoryName) {
    try {
        var statusCode = 200;

        if (highestVersion !== '2.0.0') {
            var hitsUrl = getFeatureUrl +  `&resultType=hits` + `&version=${highestVersion}`;
        } else {
            getFeatureUrl = getFeatureUrl.replace('typeName=', 'typeNames=');
            var hitsUrl = getFeatureUrl +  `&resultType=hits`;
        }
        
        var featureData = await retrieveXMLDataFromWFSUrl(hitsUrl);

        if (featureData.status) {
            statusCode = featureData.status;
        }

        if (statusCode !== 200) {
            throw new Error("The call to ' + hitsUrl + ' failed with an exit code that is not 200. Exiting...");
        }
        var hitsResults = await convertGetCapabilitiesXMLToJson(featureData.data);

        const firstKey = Object.keys(hitsResults)[0];

        // Basically just getting the number of features found in the XML parameters. Different WFS servers have different ways they name this data, either numberMatched or numberOfFeatures.
        var numberMatched = 0;
        if (hitsResults[firstKey]['$']) {
            if (hitsResults[firstKey]['$']['numberMatched']) {
                numberMatched = hitsResults[firstKey]['$']['numberMatched'];
            } else if (hitsResults[firstKey]['$']['numberOfFeatures']) {
                numberMatched = hitsResults[firstKey]['$']['numberOfFeatures'];
            }
        }

        // If we find out that we can likely download all items without paging, we query for this information and download as an xml.
        if (parseInt(numberMatched) < 100_000) {
            var directoryName = prepareDataDownloadsDirectory(outputDirectoryName);

            if (highestVersion === '2.0.0') {
                getFeatureUrl = getFeatureUrl.replace('typeName=', 'typeNames=');
            }
            featureData = await retrieveXMLDataFromWFSUrl(getFeatureUrl);
            
            xmlFileName = `dataset.xml`;
            xmlFilePath = path.join(directoryName, xmlFileName);

            fs.writeFileSync(xmlFilePath, featureData.data, 'binary');
            console.log("All done! You can find the data downloaded as dataset.xml in the data_downloads directory.");
            process.exit(0);

        }
        return false;


    } catch (error) {
        console.log("There was en error attempting to retrieve data... please try again with a different url.");
        throw error.message;
    }
}


/**
 * @param {string[]} xmlFiles - List/Array of paths of XML files we downloaded.
 * Function to merge together any XML files we have in the /data_downloads directory
 */
function mergeXMLFiles(xmlFiles) {
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
 * We pass in only the files we downloaded in this session. This makes sure we don't accidentally delete files that the user may have put there themselves.
 * @param {string[]} xmlFiles List/Array of paths of XML files we downloaded.
 */
function deleteLeftOverXMLFiles(xmlFiles) {
    for (var file of xmlFiles) {
        fs.unlink(file, (err) => {
            if (err) {
                throw err.message;
            }
        })
    }
}


/**
 * Blackbox function that calls various methods to parse a WFS url and return information necessary to download objects, if possible.
 * @param {string} wfsUrl WFS GetFeatures url in which we want to download data from.
 * @param {string} outputDirectoryName 
 */
async function downloadWfsDataInXmlFormat(wfsUrl, outputDirectoryName) {

    // In the url it looks like "&version=#.#.0". Removing the version will redirect us to the highest version that the server accepts.
    // So just in case we receive a link that has version 1.0.0, we can make sure that a higher version doesn't exist.
    // However, if the link is already 2.0.0 then we don't need to worry about removing or anything.
    var urlWithoutVersion = wfsUrl;
    const isAlreadyHighestVersion = wfsUrl.includes('version=2.0.0');
    if (!isAlreadyHighestVersion) {
        urlWithoutVersion = removeVersionFromUrl(wfsUrl);
    }

    // First, we change the url to be a GetCapabilities Request.
    var getCapabilitiesUrl = transformFeaturesUrlToCapabilitiesUrl(urlWithoutVersion);

    // We get the XML content from the GetCapabilities request
    try {
        var getCapabilitiesXML = await retrieveXMLDataFromWFSUrl(getCapabilitiesUrl);
        var capabilitiesData = getCapabilitiesXML.data;

        // Turn the GetCapabilities XML to a JSON object so its easier to work with.
        var getCapabilitiesJson = await convertGetCapabilitiesXMLToJson(capabilitiesData);

    } catch (error) {
        throw error.message;
    }


    if (isAlreadyHighestVersion) {
        var highestVersion = '2.0.0';
    } else {
        var highestVersion = parseGetCapabilitiesJsonForHighestVersion(getCapabilitiesJson);
    }

    if (!highestVersion) {
        throw new Error("The WFS server has not declared the versions it accepts");
    }

    // Based on the JSON, we check to see if the server allows for paging.
    var canPage = parseGetCapabilitiesJsonForPagingInformation(getCapabilitiesJson);


    var featuresUrlWithHighestVersion = wfsUrl;
    // If its not already a 2.0.0 link, then we should make sure we append the highest version available.
    if (!isAlreadyHighestVersion) {
        featuresUrlWithHighestVersion = urlWithoutVersion + `&version=${highestVersion}`;
    }

    var xmlFiles = [];
    var didGetResults = false;

    // If we can query for resultType=hits, lets try to get all the data without worrying about paging first...
    var resultTypeIsHits = parseGetCapabilitiesJsonForResultType(getCapabilitiesJson);
    if (resultTypeIsHits) {
        // if the getResultsOnly... method is successful, the program will exit there. Otherwise, will continue.
        didGetResults = await getResultsOnlyIfLessThan100k(featuresUrlWithHighestVersion, highestVersion, outputDirectoryName);
    } else {
        console.log("This server does not currently let us know if we can query for hits.. trying other methods...");
    }


    // If we did NOT download any data from the getResultsOnlyIfLessThan100k method, then we should try to see if we can page for the information next....
    if (!didGetResults) {
        console.log("The url has too many results to get in one call. Attempting to page...");
        if (!canPage) {
            throw new Error("The server has not declared paging as an option and we do not know if we can grab all data without paging...aborting...");
        } else {
            console.log("This server contains a lot of content, beginning to page through it, this may take a while.");
            xmlFiles = await pageThroughResults(featuresUrlWithHighestVersion, highestVersion, outputDirectoryName);
        }
    }

    // after paging, we should merge any xml files we have and then delete any leftovers XML files.
    console.log("Almost done messing with the data...");
    mergeXMLFiles(xmlFiles);

    console.log("Cleaning up some of our mess...");
    deleteLeftOverXMLFiles(xmlFiles);

    console.log("Data has been downloaded to the /data_downloads directory.");
}


module.exports = {
    downloadWfsDataInXmlFormat
}