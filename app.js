const Source = require('./lib/source')
const argentina_sources = require('./sources/argentina')
const callPython = require('./callPython')
let wfs_url = "https://data.gov.au/geoserver/colac-otway-shire-trees/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=colac-otway-shire-trees:ckan_3ce1805b_cb81_4683_8f46_e7bd2d2a3b7c&srsName=EPSG:4326"
wfs_url = "https://geo.sv.rostock.de/inspire/lcv-trees/download?service=WFS&version=2.0.0&request=GetFeature&typeNames=lcv:LandCoverUnit&srsName=EPSG:4326"


async function getArgentinaSources() {
  console.log(argentina_sources)

  // argentina_sources returns a list of 2 objects
  var source = new Source(props=argentina_sources[0], dir='test/input')

  /* 
  arg_sources[0] is:
  {
    id: 'buenos_aires',
    country: 'Argentina',
    short: 'Buenos Aires',
    download: 'http://cdn.buenosaires.gob.ar/datosabiertos/datasets/arbolado-en-espacios-verdes/arbolado-en-espacios-verdes.csv',
    info: 'https://data.buenosaires.gob.ar/dataset/arbolado-espacios-verdes',
    crosswalk: {
      ref: 'id_arbol',
      height: 'altura_tot',
      dbh: 'diametre',
      common: 'nombre_com',
      scientific: 'nombre_cie',
      family: 'nombre_fam'
    }
  }
  */

  await source.get()
  source.find()
  source.getRows(1)
}

function getWFSData(wfs_url) {
  callPython.callPythonHelper(wfs_url)
}

getWFSData(wfs_url)