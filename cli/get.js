#!/usr/bin/env node
const commandLineUsage = require('command-line-usage')
const commandLineArgs = require('command-line-args')
const { DEFAULT_OPTIONS } = require('./common')
const { loadSources } = require('../lib/load')

const OPTIONS = [
  ...DEFAULT_OPTIONS,
  {
    name: 'force', alias: 'f', type: Boolean, defaultValue: false,
    description: 'Overwrite input directory even if it is not empty.'
  }
]

const USAGE = [
  {
    header: 'example/get.js',
    content: 'Download remote files, unpack compressed or archive files, and execute shell commands to prepare source files for processing.'
  },
  {
    header: 'Options',
    optionList: OPTIONS
  }
]

// Parse command line arguments
let options
try {
  options = commandLineArgs(OPTIONS)
  console.log("options: ", options)
  if (options.help) {
    console.log(commandLineUsage(USAGE))
    process.exit(0)
  }
} catch (error) {
  console.error(`${'[Error]'.red}`, error)
  console.log(commandLineUsage(USAGE))
  process.exit(1)
}

// Load sources
const sources = loadSources(
  console.log("loading sources...")
  `${__dirname}/../sources`,
  { ids: options.ids, countries: options.countries },
  options.dir
)

// Get sources
const success = []
const failure = []
const skip = []
async function getSource(source) {
  console.log("source: ", source)
  try {
    const paths = await source.get(options.force)
    if (paths.length) {
      success.push(source.props.id)
    } else {
      skip.push(source.props.id)
    }
  } catch (error) {
    console.error(error.message)
    failure.push(source.props.id)
  }
}
async function get() {
  console.log("sources: ", sources)
  await Promise.all(sources.map(source => getSource(source)))
  if (success.length) {
    console.log(
      `${'[SUCCESS]'.green} Got ${success.length} sources:`,
      success.join(', ')
    )
  }
  if (failure.length) {
    console.error(
      `${'[ERROR]'.red} Failed to get ${failure.length} sources:`,
      failure.join(', ')
    )
  }
  if (skip.length) {
    console.log(
      `${'[SKIPPED]'.dim} Skipped ${skip.length} sources:`,
      skip.join(', ')
    )
  }
}
get()
