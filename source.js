const fs = require('fs')
const path = require('path')
const util = require('util')
const colors = require('colors')
const glob = require('glob')
const unzipper = require('unzipper')
const gdal = require('gdal-next')
const download = require('download')
const helpers = require('./helpers')

const DEFAULT_SRS_STRING = 'EPSG:4326'
const DEFAULT_SRS = gdal.SpatialReference.fromUserInput(DEFAULT_SRS_STRING)
const INPUT_NAME = 'input'
const CROSSWALK_FIELDS = {
  // Identification
  ref: {
    description: 'Feature identifier in source',
    type: gdal.OFTString
  },
  // Names
  scientific: {
    description: 'Scientific name (e.g. "Malus pumila")',
    type: gdal.OFTString
  },
  family: {
    description: 'Family (e.g. "Rosaceae")',
    type: gdal.OFTString
  },
  genus: {
    description: 'Genus (e.g. "Malus")',
    type: gdal.OFTString
  },
  species: {
    description: 'Species (e.g. "pumila")',
    type: gdal.OFTString
  },
  // TODO: Better distinguish between subspecies epithets and cultivar
  variety: {
    description: 'Subspecies, variety, form, cultivar, etc. (e.g. "asiatica")',
    type: gdal.OFTString
  },
  cultivar: {
    description: 'Cultivar (e.g. "Gala")',
    type: gdal.OFTString
  },
  common: {
    description: 'Common name (e.g. "Apple")',
    type: gdal.OFTString
  },
  description: {
    description: 'Any other name information not covered by other fields',
    type: gdal.OFTString
  },
  // Dimensions
  height: {
    description: 'Height',
    unit: 'meter',
    type: gdal.OFTReal
  },
  dbh: {
    description: 'Diameter at breast height',
    unit: 'centimetre',
    type: gdal.OFTReal,
  },
  crown: {
    description: 'Diameter of crown',
    unit: 'meter',
    type: gdal.OFTReal
  },
  // TODO: May be the same as crown
  spread: {
    description: 'Crown spread',
    unit: 'meter',
    type: gdal.OFTReal
  },
  // TODO: Clarify whether circumference of trunk or crown, convert to diameter
  circumference: {
    description: 'Circumference',
    type: gdal.OFTReal
  },
  // TODO: Clarify whether diameter of trunk or crown
  diameter: {
    description: 'Diameter',
    type: gdal.OFTReal
  },
  trunks: {
    description: 'Number of trunks',
    type: gdal.OFTInteger
  },
  // Condition
  health: {
    description: 'Health rating',
    type: gdal.OFTString,
    constraints: {
      enum: ['dead', 'poor', 'fair', 'good', 'very good', 'excellent']
    }
  },
  maturity: {
    description: 'Maturity',
    type: gdal.OFTString,
    constraints: {
      enum: ['young', 'semi-mature', 'mature', 'over-mature']
    }
  },
  structure: {
    description: 'Solidity, unlikelihood of falling',
    type: gdal.OFTString,
    constraints: {
      enum: ['failed', 'poor', 'fair', 'good']
    }
  },
  // Place
  location: {
    description: 'Where the tree is located',
    type: gdal.OFTString,
    constraints: {
      // TODO: Replace 'council' with more global term?
      enum: ['park', 'street', 'council']
    }
  },
  // Time
  planted: {
    description: 'Date of planting (or ideally, start from seed)',
    type: gdal.OFTString,
    constraints: {
      pattern: /^[0-9]{4}-[0-9]{2}-[0-9]{2}$/
    }
  },
  // TODO: Distinguish between different kinds of updates
  updated: {
    description: 'Date that data was last updated',
    type: gdal.OFTString,
    constraints: {
      pattern: /^[0-9]{4}-[0-9]{2}-[0-9]{2}$/
    }
  },
  // TODO: Only used by sources in Netherlands. Meaning unclear.
  installed: {
    description: 'Year installed',
    type: gdal.OFTInteger
  },
  age: {
    description: 'Age',
    unit: 'year',
    type: gdal.OFTInteger
  },
  // TODO: Convert to absolute years
  ule: {
    description: 'Useful life expectancy',
    unit: 'year',
    type: gdal.OFTInteger
  },
  ule_min: {
    description: 'Minimum useful life expectancy',
    unit: 'year',
    type: gdal.OFTInteger
  },
  ule_max: {
    description: 'Maximum useful life expectancy',
    unit: 'year',
    type: gdal.OFTInteger
  },
  // Other
  // TODO: Merge with description?
  note: {
    description: 'Notes',
    type: gdal.OFTString
  },
  owner: {
    description: 'Name of owner or ownership description',
    type: gdal.OFTString
  },
  // NOTE: Only Canada.regina
  value: {
    description: 'Monetary value',
    type: gdal.OFTReal
  }
}

/**
 * Download file.
 * @param {string} url Remote path to download
 * @param {string} file Local path to write
 */
async function download_file(url, file) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, await download(url));
}

/**
 * Unpack compressed or archive file to a directory.
 * @param {string} file Path of file to unpack
 * @param {string} dir Target directory
 * @param {string} format Compression or archive file format
 */
function unpack_file(file, dir, format = 'zip') {
  fs.mkdirSync(dir, { recursive: true })
  switch (format) {
    case 'zip':
      fs.createReadStream(file).pipe(unzipper.Extract({ path: dir }))
      break
    default:
      throw `Format ${format} not supported`
  }
}

/**
 * Class representing a source dataset.
 *
 * The data can be in either a remote or local file.
 * Compressed or archive files are unpacked as needed.
 */
class Source {

  /**
   * Source properties
   * @typedef {object} DatasetProperties
   * @property {string} id - Unique identifer
   * @property {string} primary - Identifier of primary dataset (in cases where 2+ datasets constitute the inventory)
   * @property {string} long - Full name for the government body (e.g. City of Melbourne)
   * @property {string} short - Display name for the government body
   * @property {string} country - Country name (in English)
   * @property {object} centre - Centre point (in case automatic placement is bad)
   * @property {number} centre.lon - Longitude in decimal degrees (EPSG:4326)
   * @property {number} centre.lat - Latitude in decimal degrees (EPSG:4326)
   * @property {string} download - Remote file path
   * @property {string} info - Page with more information
   * @property {string} language - Language tag (e.g. "en", "en-US") of data contents, especially of common names
   * @property {object} license - License
   * @property {string} license.id - SPDX identifier: https://spdx.org/licenses (e.g. CC-BY-4.0)
   * @property {string} license.name - Name (e.g. Creative Commons Attribution 4.0 International)
   * @property {string} license.url - Page with full text of license text (e.g. https://creativecommons.org/licenses/by/4.0)
   * @property {string} format - Data file format (e.g. "geojson", "csv", "shp")
   * @property {string} compression - Compression or archive file format (e.g. "zip", "tar")
   * @property {string} srs - Spatial reference system in any format supported by OGRSpatialReference.SetFromUserInput().
   *   See https://gdal.org/api/ogrspatialref.html?highlight=setfromuserinput#_CPPv4N19OGRSpatialReference16SetFromUserInputEPKc
   * @property {object} geometry - Geometry field (for non-spatial data like CSV).
   * @property {string} geometry.wkt - Name of field with WKT geometry (takes precedence)
   * @property {string} geometry.x - Name of field with x coordinate (longitude, easting)
   * @property {string} geometry.y - Name of field with y coordinate (latitude, northing)
   * @property {object} crosswalk - Crosswalk mapping to the opentrees schema.
   *   For each <key>: <value>, <key> is the new field name and <value> is either
   *   the old field name (string) or a function called as f(feature.properties).
   * @property {function} delFunc - Function called as f(feature.properties) for each feature (before crosswalk).
   *   Feature is excluded from output if function returns true.
   * @property {function} coordsFunc - Function called as f(features.properties) for each feature (before crosswalk).
   *   Returns an array of feature coordinates [x, y].
   */

  /**
   * Create a dataset.
   * @param {DatasetProperties} props - Dataset properties
   * @param {string} dir - Working directory
   * @param {boolean} overwrite - Whether to overwrite existing files
   * @param {boolean} exit - Whether to throw (exit on) or print errors
   */
  constructor(props, dir, overwrite = false, exit = false) {
    this.props = props
    this.dir = dir
    this.overwrite = overwrite
    this.exit = exit
    // Apply defaults
    // NOTE: Alternatively, call get_* in methods
    this.props.compression = this.get_compression()
    this.props.format = this.get_format()
  }

  /**
   * Validate source properties.
   * @param {boolean} error - Whether to raise errors
   * @return {string[]} Errors
   */
  validate(error = false) {
    var errors = []
    const props = this.props
    // Required fields
    if (!props.id || typeof (props.id) !== 'string') {
      errors.push(`Invalid id: ${props.id}`)
    }
    if (!props.download || typeof (props.download) !== 'string') {
      errors.push(`Invalid download: ${props.download}`)
    }
    // format
    if (props.format &&
      !['csv', 'geojson', 'shp', 'kml', 'gml'].includes(props.format)) {
      errors.push(`Invalid format: ${props.format}`)
    }
    // compression
    if (props.compression && !['zip'].includes(props.compression)) {
      errors.push(`Invalid compression: ${props.compression}`)
    }
    // crosswalk
    if (props.crosswalk) {
      Object.keys(props.crosswalk).forEach(key => {
        if (!Object.keys(CROSSWALK_FIELDS).includes(key)) {
          errors.push(`Unsupported crosswalk property: ${key}`)
        }
        const value = props.crosswalk[key]
        if (!['string', 'function'].includes(typeof (value))) {
          errors.push(`Invalid type for crosswalk.${key}: ${typeof (value)}`)
        }
      })
    }
    // geometry
    if (props.geometry) {
      if (!(typeof (props.geometry) === 'object' &&
        (typeof (props.geometry.wkt) === 'string' ||
          (typeof (props.geometry.x) === 'string' && typeof (props.geometry.y) === 'string')))) {
        errors.push(`Invalid geometry: ${JSON.stringify(props.geometry)}`)
      }
    }
    // srs
    if (props.srs) {
      try {
        gdal.SpatialReference.fromUserInput(props.srs)
      } catch (err) {
        errors.push(`Invalid srs: ${props.srs}`)
      }
    }
    if (error) {
      errors.forEach(msg => this.error(msg))
    } else {
      return errors
    }
  }

  /**
   * Print message to console.
   * @param {string} msg - Message
   */
  log(msg) {
    const tag = colors.cyan(`[${this.props.id}]`)
    console.log(`${tag} ${msg}`)
  }

  /**
   * Print warning to console.
   * @param {string} msg - Message
   */
  warn(msg) {
    const tag = colors.yellow(`[${this.props.id}]`)
    console.log(`${tag} ${msg}`)
  }

  /**
   * Throw or print error to console.
   * @param {string} msg - Message
   * @param {boolean} exit - Whether to throw or print the error
   */
  error(msg, exit = this.exit) {
    const tag = colors.red(`[${this.props.id}]`)
    if (exit) {
      throw new Error(`${tag} ${msg}`)
    } else {
      console.error(`${tag} ${msg}`)
    }
  }

  /**
   * Get format of remote file (unpacked).
   *
   * Either the provided format or guessed from the download url (file
   * extension, parameters) if these give a unique solution.
   *
   * @return {string} Compression format
   */
  get_format() {
    var format = this.props.format
    if (!format) {
      if (this.props.compression) {
        const format = 'shp'
        this.warn(`Assuming format ${format} for compression ${this.props.compression}`)
        return format
      }
      if (this.props.download) {
        const url = this.props.download.toLowerCase()
        const matches = [
          // file extension
          url.match(/\.([^\.\/\?\#]+)(?:$|\?|\#)/),
          // parameters
          url.match(/[\?\&\#](?:f|format|outputformat)=([^\&\s\#]+)/)]
        const formats = matches.filter(x => x != null && !['zip'].includes(x)).
          map(x => x[1]).
          filter((v, i, x) => x.indexOf(v) === i)
        if (formats.length == 1) {
          format = formats[0]
        }
        if (formats.length != 1) {
          // TODO: Move compression formats to constant
          this.error(`Failed to determine format from url: ${this.props.download}`)
        }
        if (format === 'json') {
          format = 'geojson'
        }
      }
    }
    return format
  }

  /**
   * Get compression format of remote file.
   *
   * Either the provided compression format or the file extension of the
   * download url, if it is a recognized compression format.
   *
   * @return {string} Compression format
   */
  get_compression() {
    var compression = this.props.compression
    if (!compression) {
      const url = this.props.download.toLowerCase()
      const matches = url.match(/\.([^\.\/\?\#]+)(?:$|\?|\#)/)
      if (matches && matches.length && matches[1] === 'zip') {
        // TODO: Move compression formats to constant
        compression = matches[1]
      }
    }
    return compression
  }

  /**
   * Get local path for remote file.
   * @return {string} File path
   */
  get_download_path() {
    const format = this.props.compression ? this.props.compression : this.props.format
    return path.join(this.dir, INPUT_NAME, `${INPUT_NAME}.${format}`)
  }

  /**
   * Find path of input file.
   * @param {boolean} error - Whether to raise error if no input found
   * @return {string} File path (if found) or undefined
   */
  find_input_path(error = true) {
    var input_path = null
    if (this.props.compression) {
      const pattern = path.join(this.dir, INPUT_NAME, `**/*.${this.props.format}`)
      const files = glob.sync(pattern)
      if (files.length <= 1) {
        input_path = files[0]
      } else {
        this.error(`${pattern} matches ${files.length} files`);
      }
    } else {
      input_path = this.get_download_path()
      if (!fs.existsSync(input_path)) {
        input_path = null
      }
    }
    if (!input_path && error) {
      this.error(`No input found`)
    } else {
      return input_path
    }
  }

  /**
   * Get input spatial reference system (SRS) as a string.
   *
   * Either the provided SRS, the SRS of the layer (as proj4 string),
   * or the default SRS.
   *
   * @param {gdal.Layer} layer - Feature layer
   * @return {string} Input SRS
   */
  get_srs_string(layer) {
    var srs = this.props.srs
    if (!srs && layer && layer.srs) {
      srs = layer.srs.toProj4()
    }
    if (!srs) {
      srs = DEFAULT_SRS_STRING
      this.warn(`Assuming default SRS: ${srs}`)
    }
    return srs
  }

  /**
   * Get input spatial reference system (SRS).
   *
   * Either the provided SRS (passed to gdal.SpatialReference.fromUserInput),
   * the SRS of the layer, or the default SRS.
   * 
   * @param {gdal.Layer} layer - Feature layer
   * @return {gdal.SpatialReference} Input SRS
   */
  get_srs(layer) {
    const srs = this.get_srs_string(layer)
    return gdal.SpatialReference.fromUserInput(srs)
  }

  /**
   * Get input geometry definition.
   * 
   * Either the provided geometry or guessed from layer field names.
   * 
   * @param {gdal.Layer} layer - Feature layer
   * @return {object} Layer geometry
   */
  get_geometry(layer) {
    var geometry = this.props.geometry
    if (!geometry && layer) {
      const matches = helpers.guess_geometry_fields(layer)
      if (matches.wkt.length) {
        if (matches.wkt.length > 1) {
          this.warn(`Using first of many WKT fields: ${matches.wkt.join(', ')}`)
        }
        geometry = { wkt: matches.wkt[0] }
      } else if (matches.x.length && matches.y.length) {
        if (matches.x.length > 1) {
          this.warn(`Using first of many X fields: ${matches.x.join(', ')}`)
        }
        if (matches.y.length > 1) {
          this.warn(`Using first of many Y fields: ${matches.y.join(', ')}`)
        }
        geometry = { x: matches.x[0], y: matches.y[0] }
      } else {
        this.error(`Failed to guess geometry fields: ${util.inspect(matches)}`)
      }
    }
    return geometry
  }

  /**
  * Write VRT (OGR Virtual Format) for feature layer.
  *
  * Relevant only for tabular data with feature geometry in fields.
  * Writes the file to the path of the layer with '.vrt' added.
  *
  * See https://gdal.org/drivers/vector/vrt.html
  *
  * @param {gdal.Layer} layer - Feature layer
  * @param {boolean} keep_geometry_fields - Whether VRT file should return
  *   geometry fields as regular feature fields
  * @return {string} Path to VRT file.
  */
  write_vrt(layer, keep_geometry_fields = false) {
    const srs = this.get_srs_string(layer)
    const geometry = this.get_geometry(layer)
    // Build <GeometryField> attributes
    var attributes
    if (geometry.wkt && typeof geometry.wkt === 'string') {
      attributes = `encoding="WKT" field="${geometry.wkt}"`
    } else if (
      geometry.x && typeof geometry.x === 'string' &&
      geometry.y && typeof geometry.y === 'string') {
      attributes = `encoding="PointFromColumns" x="${geometry.x}" y="${geometry.y}"`
    } else {
      this.error(`Invalid geometry: ${util.inspect(geometry)}`)
    }
    // Build VRT
    var layer_path = layer.ds.description
    const basename = path.parse(layer_path).base
    const vrt =
      `<OGRVRTDataSource>
       <OGRVRTLayer name="${layer.name}">
           <SrcDataSource relativeToVRT="1">${basename}</SrcDataSource>
           <GeometryType>wkbPoint</GeometryType>
           <LayerSRS>${srs}</LayerSRS>
           <GeometryField ${attributes} reportSrcColumn="${keep_geometry_fields}"/>
       </OGRVRTLayer>
     </OGRVRTDataSource>`
    // Write VRT
    const vrt_path = `${layer_path}.vrt`
    fs.writeFileSync(vrt_path, vrt)
    return vrt_path
  }

  /**
   * Download and unpack file.
   * @param {boolean} rm - Whether to remove pack file after unpacking
   */
  get(rm = true) {
    this.download().then(() => source.unpack())
  }

  /**
   * Download remote file.
   * @return {Promise} Promise that resolves once file is downloaded.
   */
  download() {
    if (this.props.download && (this.overwrite || !this.find_input_path())) {
      this.log(`Downloading ${this.props.download}`)
      return download_file(this.props.download, this.get_download_path())
    } else {
      return Promise.resolve()
    }
  }

  /**
   * Unpack file.
   * @param {boolean} rm - Whether to remove pack file after unpacking
   */
  unpack(rm = true) {
    if (this.props.compression && (this.overwrite || !this.find_input_path(false))) {
      const unpack_path = this.get_download_path()
      this.log(`Unpacking ${unpack_path}`)
      unpack_file(unpack_path, path.join(this.dir, INPUT_NAME), this.props.compression)
      if (rm) {
        fs.unlinkSync(unpack_path)
      }
    }
  }

  /**
   * Process input file and write to output.
   * 
   * @param {string} file - Output file path
   * @param {string} format - Name of GDAL driver (e.g. "csv", "geojson")
   * @param {object} options
   * @param {boolean} [options.centroids=false] - Whether to reduce non-point
   *  geometries to centroids
   * @param {boolean} [options.keep_invalid=false] - Whether to keep features
   *  with empty or invalid geometries
   * @param {boolean} [options.keep_fields=false] - Whether to keep original
   *  feature fields
   * @param {boolean} [options.keep_geometry_fields=false] - Whether to keep
   *  original feature fields reporting geometry. Applies only to layers for
   *  which a VRT file had to be written.
   * @param {string} [options.prefix='_'] - String to append to original
   *  field names (if kept)
   */
  process(file, format = 'csv', options = {}) {
    options = {
      ...{
        centroids: false,
        keep_invalid: false,
        keep_fields: false,
        keep_geometry_fields: false,
        prefix: '_'
      },
      ...options
    }
    if (!this.overwrite && fs.existsSync(file)) {
      return
    }
    var input_path = this.find_input_path()
    // Read input
    this.log(`Processing ${input_path}`)
    var input = gdal.open(input_path)
    if (input.layers.count() > 1) {
      this.warn(`Using first of ${input.layers.count()} layers`)
    }
    var input_layer = input.layers.get(0)
    if (!input_layer.features.count()) {
      this.warn(`Skipping: Layer has no features`)
      return
    }
    if (!input_layer.features.first().getGeometry() && !this.props.coordsFunc) {
      // Write (and then read) VRT file with geometry definition
      this.log(`Writing VRT file`)
      input_path = this.write_vrt(input_layer, options.keep_geometry_fields)
      input = gdal.open(input_path)
      input_layer = input.layers.get(0)
    }
    // Prepare input schema
    var input_schema = input_layer.fields.map(field => field)
    /**
     * NOTE: Confusing gdal bindings handling of date/time fields
     * - Fields detected as date/time are read as objects, not strings
     * - Cannot yet set date/time field from date/time object, only strings
     * (see https://github.com/naturalatlas/node-gdal/issues/144)
     * HACK:
     * - Set output date/time fields as string
     * - Convert input date/time fields to string
     */
    var string_crosswalk = {}
    input_schema = input_schema.map(field => {
      const formatter = helpers.gdal_string_formatters[field.type]
      if (formatter) {
        string_crosswalk[field.name] = x => formatter(x[field.name])
        field.type = gdal.OFTString
      }
      return field
    })
    // Prepare output schema
    var output_schema = []
    if (this.props.crosswalk) {
      for (const key in this.props.crosswalk) {
        // NOTE: Output as string to preserve malformed values
        output_schema.push(new gdal.FieldDefn(key, gdal.OFTString))
      }
    }
    if (options.keep_fields) {
      input_schema.forEach(field => {
        field.name = `${options.prefix}${field.name}`
        output_schema.push(field)
      })
    }
    // Prepare output
    const driver = gdal.drivers.get(format)
    if (!driver) {
      this.error(`Unsupported output format: ${format}`)
    }
    var output
    if (driver.description === 'CSV') {
      // GEOMETRY=AS_WKT writes WKT geometry to 'WKT' field
      // GEOMETRY=AS_XY, CREATE_CSVT=YES, OGR_WKT_PRECISION=6 not supported
      // TODO: Write VRT (if needed)
      output = driver.create(
        file, 0, 0, 0, gdal.GDT_Byte, ['GEOMETRY=AS_WKT'])
    } else {
      output = driver.create(file)
    }
    var output_type
    if (options.centroids || input_layer.geomType == gdal.wkbNone) {
      output_type = gdal.wkbPoint
    } else {
      output_type = input_layer.geomType
    }
    const output_layer = output.layers.create(input_layer.name, DEFAULT_SRS,
      output_type)
    output_layer.fields.add(output_schema)
    // Determine if a coordinate transformation is needed
    // TODO: Make and test transform, check whether points are unchanged?
    const input_srs = this.get_srs(input_layer)
    var transform
    if (input_srs.isSame(DEFAULT_SRS) ||
      (input_srs.isSameGeogCS(DEFAULT_SRS) &&
        (input_srs.isProjected() == DEFAULT_SRS.isProjected()))) {
      transform = null
    } else {
      transform = new gdal.CoordinateTransformation(input_srs, DEFAULT_SRS)
    }
    // Populate output
    var input_feature = input_layer.features.first()
    for (
      var input_feature = input_layer.features.first();
      input_feature;
      input_feature = input_layer.features.next()) {
      // Fields
      const input_fields = input_feature.fields.toObject()
      if (this.props.delFunc && this.props.delFunc(input_fields)) {
        continue
      }
      const output_feature = new gdal.Feature(output_layer)
      var output_fields = helpers.map_object(input_fields, string_crosswalk,
        true, '')
      output_fields = helpers.map_object(
        output_fields,
        this.props.crosswalk ? this.props.crosswalk : {},
        options.keep_fields, options.prefix)
      output_feature.fields.set(output_fields)
      // Geometry
      var input_geometry
      if (this.props.coordsFunc) {
        const coords = this.props.coordsFunc(input_fields)
        if (Array.isArray(coords) && coords.length == 2) {
          input_geometry = new gdal.Point(coords[0], coords[1])
          input_geometry.srs = input_srs
        } else {
          this.warn(`Invalid parsed coordinates at ${input_feature.fid}: ${util.inspect(coords)}`)
          if (!options.keep_invalid) continue
        }
      } else {
        input_geometry = input_feature.getGeometry()
      }
      if (input_geometry) {
        if (options.centroids && input_geometry.wkbType != gdal.wkbPoint) {
          input_geometry = input_geometry.centroid()
        }
        if (input_geometry.isValid() &&
          input_geometry.x && input_geometry.y &&
          isFinite(input_geometry.x) && isFinite(input_geometry.y)) {
          if (transform) {
            try {
              input_geometry.transform(transform)
            } catch (error) {
              this.warn(`Invalid geometry at ${input_feature.fid}: ${input_geometry.x}, ${input_geometry.y} (x, y)`)
              if (!options.keep_invalid) continue
            }
          }
        } else {
          this.warn(`Invalid geometry at ${input_feature.fid}: ${input_geometry.x}, ${input_geometry.y} (x, y)`)
          if (!options.keep_invalid) continue
        }
        output_feature.setGeometry(input_geometry)
      } else {
        this.warn(`Empty geometry at ${input_feature.fid}`)
        if (!options.keep_invalid) continue
      }
      // TODO: flush after each n features
      output_layer.features.add(output_feature)
    }
    // Write
    output.close()
    input.close()
    return file
  }
}

module.exports = Source
