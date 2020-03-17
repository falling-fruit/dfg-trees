#!/usr/bin/env node --max-old-space-size=8192
/* jshint esnext:true */
const fs = require('fs');
const colors = require('colors');
const download = require('download');
const extract = require('extract-zip');
const sources = require('./sources');
const child_process = require('child_process');
const { match } = require('./utils');

['./data', './data/unzip', './tmp'].forEach(dir => {
    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir);
    }
});

let skipCount = 0;
sources.forEach(function(source) {
    var urls = source.download;
    if (!Array.isArray(urls)) {
        urls = [urls];
    }
    urls.forEach(url => {
        // hmm, 'format' is used for saving the file, but also knowing what file to import.
        var format = source.zip ? 'zip' : source.format;
        if (!format) {
            format = match(url, /\.([a-z]+)$/i, 1);
        }
        if (source.format === 'json') {
            source.format = 'geojson';
        }
    
            
        if (source.keepExtension) {
            format = url.match(/\.([^.]+)$/)[1];
        }
        
        var filename = 'data/' + source.id + '.' + format;
        if (!fs.existsSync(filename)) {
            console.log('Downloading '.cyan + /*source.download + ' to ' + */ filename);

            
            download(url).then(data => {
                fs.writeFileSync(filename, data);            
                console.log('Downloaded '.green + filename);
                if (format === 'zip' || source.zip) {
                    console.log('Unzipping ' + filename);
                    // extract(filename, { dir: process.cwd() + `/data/unzip/${source.id}` }, function(err) {
                    //     if (err) {
                    //         console.error('Error unzipping '.error + filename + ': ' + err);
                    //     } else {
                    //         console.log('Unzipped '.green + filename);
                    //     }
                    // });
                    try {
                        child_process.execSync(
                            `unzip -d ${process.cwd()}/data/unzip/${source.id} "${filename}"`,
                            { stdio: 'inherit' }
                        );
                        console.log('Unzipped '.green + filename);
                    } catch (e) {
                        console.error('Error unzipping '.error + filename + ': ' + err);
                    }

                }
            }).catch(function(err) {
                console.error((filename + ': ').red + err);
            });
        } else {
            skipCount++;
            // console.log('(Skip ' + filename + ')');
        }
    });
});
console.log(`Skipped ${skipCount} sources that already existed on disk.`);
