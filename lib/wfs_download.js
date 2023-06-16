const axios = require('axios');
const { XMLParser} = require("fast-xml-parser");
const parser = new XMLParser();
const { parseString } = require('xml2js');
const fs = require('fs');
const path = require('path');


// only for development purposes. Please delete if/when creating a pull request for production (mainline).
const wfs_url_that_does_not_support_paging = "https://geodienste.halle.de/opendata/fa3930b7-b3ed-b3fc-20d9-2fc8fd054b0e?service=WFS&version=1.1.0&request=GetFeature&typeName=fa3930b7-b3ed-b3fc-20d9-2fc8fd054b0e&srsName=EPSG:4326";
const wfs_url_that_accepts_a_higher_version = "https://gis.gouda.nl/geoserver/BOR/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=BOR:V_BOMEN_ALLES&srsName=EPSG:4326";
const wfs_url_with_less_than_10_000_results = "https://cugir.library.cornell.edu/geoserver/cugir/wfs?service=WFS&version=2.0.0&request=GetFeature&typeName=cugir009100&srsName=EPSG:4326";
const wfs_url_with_more_than_100_000_results = "https://maps.groningen.nl/geoserver/geo-data/ows?service=WFS&version=2.0.0&request=GetFeature&typeName=geo-data:Bomen+gemeente+Groningen&srsName=EPSG:4326"
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
 * @TODO - (REQUIRED) CLEANUP.
 * @TODO - (REQUIRED) change parameter name to generic url name (and update function in general) since we can pass in GetFeature and GetCapabilities urls here.
 * 
 * Make a request to retrieve the XML data from the GetCapabilities request. Then utilize the fast-xml-parser library to turn this XML data into a JSON object.
 * @param {string} capabilitiesUrl WFS GetCapabilities Url with version removed.
 * @returns
 */

async function retrieveXMLDataFromWFSUrl(capabilitiesUrl) {
    try {
        const response = await axios.get(capabilitiesUrl);
        return response

    } catch (error) {
        console.log("error during get request: ", capabilitiesUrl)
        //console.error('Error during GetCapabilities request: ', error.message);
        //throw error;
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
 * @todo: (REQUIRED) cleanup
 * 
 * Parsing the JSON we have and finding out if we can page or not by looking for the 'ImplementResultPaging'
 * 
 * @param {JSON} getCapabilitiesJSON 
 */
function parseGetCapabilitiesJsonForPagingInformation(getCapabilitiesJSON) {
    var possibleBeginningKeys = ['WFS_Capabilities', 'wfs:WFS_Capabilities'];
    //const keysInJsonObject = Object.keys(getCapabilitiesJSON);

    for (key of possibleBeginningKeys) {
        if (typeof getCapabilitiesJSON[key] !== 'undefined') {
            var operationsMetadata = getCapabilitiesJSON[key]['ows:OperationsMetadata'][0];
            var constraintsList = operationsMetadata['ows:Constraint'];
            for (constraint of constraintsList) {
                if (constraint['$'].name === 'ImplementsResultPaging') {
                    return constraint['ows:DefaultValue'][0];
                }
            }
            console.log("The WFS server has not declared paging capabilities.");
            return 0;
        } else {
            console.log("could not get " + key + " trying the next one.");
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
    console.log(directoryName);

    if (!fs.existsSync(directoryName)) {
        console.log("creating data directory...");
        fs.mkdirSync(directoryName, {recursive: true});
    }
    return directoryName;
}

/**
 * @todo: (QOL) Change name of this function to make it clear that we are returning a boolean. Can be named like.. shouldContinuePaging or whatever...
 * 
 * @param {string} xmlString 
 * @returns {boolean} boolean indicating if we should continue paging or not
 */
function getNextTokenFromResult(xmlString) {
    jsonObj = convertGetCapabilitiesXMLToJson(xmlString);
    return jsonObj.then(jsonData => {
        if (jsonData['wfs:FeatureCollection']['$'].next) {
            console.log('The "next" parameter is available and populated');
            return true;
          } else {
            console.log('The "next" parameter is either missing or empty.');
            return false;
          }
    });
}


/**
 * @todo: (REQUIRED) cleanup
 * @todo: (REQUIRED) this currently only works for 2.0.0 urls. Lets ensure we capture 1.0.0 and 1.1.0 functionality. Those versions page by using the "&maxFeatures" parameter in the URL, not the "&count". So add a case
 *          to use either or. We can likely get the version we need from the JSON object we get from the capabilities request.
 * @todo: (KINDA QOL?) if we decide to go the route of not doing the GetCapabilities request to see if we can page first, then we need to ensure that if we cannot retrieve ALL data from a URL then to abort the request and throw an error back.
 *          We can do this by making a GetFeatures request and then trying to page. When we make a query with paging parameters, there should be a "next" token. If the token is not there, then we need to be sure that we grabbed all data. There should be
 *          "numberReturned" and "numberMatched" parameters at the top of the GetFeatures response XML. If numberReturned < 100,000 then we're good. If not (or if the parameter is missing all together) 
 *          then we should not accept the url and throw an error since there is no way to grab all the data.
 * 
 * Page through the WFS results and save the XMLs to the /data_downloads directory.
 * 
 * @param {string} getFeatureUrl 
 */
async function pageThroughResults(getFeatureUrl) {
    var max_results_per_page = 1000; // FEEL FREE TO CHANGE THIS IF USING A WFS URL LINK THAT HAS MORE THAN 1500 ITEMS. VIEW THE AVAILABLE WFS URLS AT THE TOP OF THIS FILE.
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
            var pagingUrl = getFeatureUrl + `&count=${max_results_per_page}&startIndex=${startIndex}`;

            console.log("paging from url: ", pagingUrl);
            
            featureData = await retrieveXMLDataFromWFSUrl(pagingUrl);
            statusCode = featureData.status;

            if (statusCode !== 200) {
                shouldContinue = false;
            }

            // first check if nextToken is available. If the next token exists then that means we can page and should continue.
            await getNextTokenFromResult(featureData.data).then(result => {
                nextToken = result;
            });

            if (nextToken == false) {
                shouldContinue = false;
            }

            xmlFileName = `dataset${iterations}.xml`;
            console.log("xmlfilename: ", xmlFileName);
            xmlFilePath = path.join(directoryName, xmlFileName);
            console.log("xml file path: ", xmlFilePath);

            // I have this list of the paths so maybe if we need to delete leftover files manually, we know exactly which files we need to delete by just iterating
            // through the list. may end up being unnecessary in the end so we can remove it later but not a terrible idea to have for now.
            xmlFiles.push(xmlFilePath);

            fs.writeFileSync(xmlFilePath, featureData.data, 'binary');
            console.log("done writing the file to the xml file path");

            iterations = iterations + 1;
            startIndex = startIndex + max_results_per_page
        }
    } catch (error) {
        console.error('Error during pageThroughResults: ', error.message);
        throw error;
    }
}

/**
 * @todo: (REQUIRED) implement
 * Function to merge together any XML files we have
 */
function mergeXMLFiles() {

}


/**
 * @todo: (QOL) Switch the capabilities and version removing... so that we remove the version from the url first and then yeah...
 * @todo: (QOL) possibly remove the getCapabilities call (don't delete the actual helper methods above though).. can just automatically call pageThrough results and if we can't page then we can handle that later...
 * @todo: (REQUIRED) cleanup
 * @todo: (REQUIRED) as part of the 1st QOL listed here. Currently, if we get a WFS url that actually accepts a higher version, we don't use that highest version, we actually just call using the passed in url which will contain the "old" version. We should make sure that
 *          if we end up sticking with the GetCapabilities request that we end up querying based on the highest version and not the "old" one (assuming they are different).
 * 
 * Blackbox function that calls various methods to parse a WFS url and return information necessary to download objects, if possible.
 * @param {string} wfsUrl WFS GetFeatures url in which we want to download data from.
 */
async function downloadWfsDataInXmlFormat(wfsUrl) {
    // First, we change the url to be a GetCapabilities Request. As mentioned in the TODO, we can likely skip these next few methods and just go straight to paging (via the pageThroughResults method).
    var getCapabilitiesUrl = transformFeaturesUrlToCapabilitiesUrl(wfsUrl);
    console.log("getting paging capability from: ", getCapabilitiesUrl);

    // After we get the GetCapabilities URL, we will remove the version from the URL. In the url it looks like "&version=#.#.0". Removing the version will redirect us to the highest version that the server accepts.
    // So just in case we receive a link that has version 1.0.0, we can make sure that a higher version doesn't exist.
    var getCapabilitiesUrlWithoutVersion = removeVersionFromUrl(getCapabilitiesUrl);
    console.log("get capabilities without version: ", getCapabilitiesUrlWithoutVersion);


    // We get the XML content from the GetCapabilities request
    var getCapabilitiesXML = await retrieveXMLDataFromWFSUrl(getCapabilitiesUrlWithoutVersion)
    var capabilitiesData = getCapabilitiesXML.data

    // Turn the GetCapabilities XML to a JSON object so its easier to work with.
    var getCapabilitiesJson = await convertGetCapabilitiesXMLToJson(capabilitiesData)


    // Based on the JSON, we check to see if the server allows for paging.
    var canPage = parseGetCapabilitiesJsonForPagingInformation(getCapabilitiesJson);
    console.log("can page: ", canPage);

    // If we can page... well.. we will page.
    if (canPage == 'TRUE') {
        console.log("paging....");
        pageThroughResults(wfsUrl);
    }

}

/**
 * @todo: (QOL) add the ability to turn the XML to a CSV if we want or we can check if the server can send us a CSV.. we can do this by utilizing output format data in the GetCapabilities URL. May be helpful in the future. Not super important.
 * @todo: (REQUIRED) test more using the wfs link at the top of this file.
 * @todo: (REQUIRED) add proper JS docs to properly describe functions and declare parameter + return types.
 * @todo: (REQUIRED) google proper JS best practices (asyncs, function names, variable names, etc)
 * @todo: (REQUIRED) implement the mergeXMLFiles() functionality. If the merge that happens doesn't remove the leftover XML files, we need a function that will remove those XML files as well.
 * @todo: (REQUIRED) cleanup whatever functions.. remove extra console.log() statements, add try, catch's where applicable etc.
 */
downloadWfsDataInXmlFormat(wfs_url_with_less_than_10_000_results)