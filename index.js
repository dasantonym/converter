import fs from 'fs-extra';
import Debug from 'debug';
import path from 'path';
import Promise from 'bluebird';
import { spawn } from 'child_process';
import s3 from 's3';
import slug from 'slug';
import md5file from 'md5-file/promise';
import ffmpeg from 'fluent-ffmpeg';
import random from 'secure-random';
import sharp from 'sharp';
import fileType from 'file-type';
import readChunk from 'read-chunk';
import gm from 'gm';
import crypto from 'crypto';

import * as config from './config.json';

const debug = Debug('converter'),
    filesToDo = [],
    filesErrored = [];

const s3client = s3.createClient({
    s3Options: {
        accessKeyId: config.s3key,
        secretAccessKey: config.s3secret
    }
});
s3client.s3.addExpect100Continue = function() {};

const parseEntry = function (fname) {
    if (path.basename(fname).substr(0,1) === '.') {
        return;
    }
    return fs.stat(fname)
        .then(stats => {
            if (stats.isDirectory()) {
                return fs.readdir(fname)
                    .then(entries => {
                        return Promise.map(entries, entry => {
                            return parseEntry(path.join(fname, entry));
                        });
                    });
            } else {
                if (config.extensions.indexOf(path.extname(fname).toLowerCase()) !== -1) {
                    filesToDo.push(fname);
                }
            }
        });
};

const sanitizePath = (filepath) => {
    return filepath.replace(/\s/g, '\\ ').replace(/&/g, '\\&');
};

const getMP4Command = (inFile, outFile, audioCodec) => {
    return `${config.ffmpeg} -y -i ${sanitizePath(inFile)} -acodec ${audioCodec} -b:a 192k ` +
        `-vcodec libx264 -vf scale=960:-1 -pix_fmt yuv420p -profile:v baseline -level 3 ` +
        `-strict -2 ${sanitizePath(outFile)}`;
};

const getWebmCommand = (inFile, outFile) => {
    return `${config.ffmpeg} -y -i ${sanitizePath(inFile)} -vcodec libvpx-vp9 -vf scale=960:-1 -b:v 1M ` +
        `-acodec libvorbis ${sanitizePath(outFile)}`;
};

const processFile = (inFile, outFile, command) => {
    return fs.exists(outFile)
        .then(exists => {
            if (exists) {
                return fs.stat(outFile)
                    .then(stats => {
                        return stats.size !== 0;
                    });
            }
            return exists;
        })
        .then(exists => {
            if (!exists) {
                return;
            }
            let durationIn, durationOut;
            return getDuration(inFile)
                .then(duration => {
                    durationIn = duration;
                    return getDuration(outFile);
                })
                .then(duration => {
                    durationOut = duration;
                    if (typeof durationIn === 'string' && typeof durationOut === 'string') {
                        if (durationIn !== durationOut) {
                            debug(`Unequal durations IN: ${durationIn} OUT: ${durationOut}`);
                        }
                    }
                    return isDurationEqual(durationIn, durationOut, 1);
                });
        })
        .then(exists => {
            if (exists) {
                debug('skipping...');
                return;
            }
            const args = command.split(' ');
            return new Promise((resolve, reject) => {
                let stdout = '', stderr = '';
                const ffmpeg = spawn(args.shift(), args, { shell: true });
                ffmpeg.stdout.on('data', (data) => {
                    stdout += data.toString();
                });
                ffmpeg.stderr.on('data', (data) => {
                    stderr += data.toString();
                });
                ffmpeg.on('close', (code) => {
                    if (code !== 0) {
                        debug(`FFMpeg exited with code ${code}`);
                        debug(stderr);
                        return reject(new Error('FFMpeg failed'));
                    }
                    debug(stdout);
                    debug(`Processed ${inFile}`);
                    resolve();
                    ffmpeg.stdin.end();
                });
            });
        })
        .catch(err => {
            debug(`Failed to process ${inFile} to outfile: ${outFile} with error: ${err.message}`);
            filesErrored.push({ err: err, infile: inFile, outfile: outFile });
        });
};

const getDuration = (inFile) => {
    return new Promise((resolve, reject) => {
        let stdout = '', stderr = '';
        const probe = spawn(config.ffprobe, [sanitizePath(inFile)], { shell: true });
        probe.stdout.on('data', (data) => {
            stdout += data.toString();
        });
        probe.stderr.on('data', (data) => {
            stderr += data.toString();
        });
        probe.on('close', (code) => {
            if (code !== 0) {
                debug(`FFProbe exited with code ${code}`);
                reject(new Error('FFProbe failed'));
            }
            const lines = stderr.split('\n');

            while (lines.length > 0 && lines[0].indexOf('Duration:') === -1) {
                lines.shift();
            }
            const timecode = lines.length >= 2 ? lines[0].split(', ')[0].split(': ')[1] : undefined;
            debug(stdout);
            resolve(typeof timecode === 'string' ? timecode.split('.')[0].split(':').map(elem => { return parseInt(elem); }) : timecode);
            probe.stdin.end();
        });
    });
};

const isDurationEqual = (dur1, dur2, toleranceSec = 0, toleranceMin = 0, toleranceHour = 0) => {
    if (!Array.isArray(dur1) || !Array.isArray(dur2)) {
        return false;
    }
    return Math.abs(dur1[0] - dur2[0]) <= toleranceHour &&
        Math.abs(dur1[1] - dur2[1]) <= toleranceMin &&
        Math.abs(dur1[2] - dur2[2]) <= toleranceSec;
};

const uploadFile = (file, bucket, key) => {
    return new Promise((resolve, reject) => {
        const upload = s3client.uploadFile({
            localFile: file,
            s3Params: {
                Bucket: bucket,
                Key: key
            }
        });
        upload.on('error', function(err) {
            debug(`Upload failed for file ${file} error: ${err.message}`);
            resolve(err);
        });
        upload.on('progress', function() {
            Debug('converter:progress')(`Upload progress ${upload.progressMd5Amount} ${upload.progressAmount} ${upload.progressTotal}`);
        });
        upload.on('end', function() {
            debug(`Upload completed for file ${file}`);
            resolve();
        });
    });
};

const fakeUploadFile = (file, bucket, key) => {
    return fs.ensureDir(path.dirname(path.join(config.outPath, key)))
    .then(() => {
        return fs.copy(file, path.join(config.outPath, key));
    });
};

const makeS3Key = (file) => {
    let newpath = file.replace(config.outPath + '/', ''),
        extname = path.extname(newpath),
        basename = path.basename(newpath, extname),
        dirname = path.dirname(newpath).split('/').splice(0,3);
    dirname = dirname.map(elem => { return slug(elem, { lower: true, replacement: '_' }); }).join('/');
    const checksum = crypto.createHash('md5').update(dirname + basename).digest('hex');
    return Promise.resolve(`${dirname}/${slug(basename, { lower: true, replacement: '_' })}-${checksum}${extname}`);
    /*
    return md5file(file).then(checksum => {
        return `${dirname}/${slug(basename, { lower: true, replacement: '_' })}-${checksum}${extname}`;
    });
    */
};

const getThumbNail = (file) => {
    const gifFolder = path.join(path.dirname(file), random.randomBuffer(8).toString('hex'));
    return fs.ensureDir(gifFolder)
        .then(() => {
            debug('Extract screenshots with ffmpeg');
            return new Promise((resolve, reject) => {
                ffmpeg(file).screenshots({
                    count: 50,
                    folder: gifFolder
                })
                .on('filenames', (fnms) => {
                    debug('Extracting ' + fnms.length + ' sample frames...')
                })
                .on('end', () => {
                    debug('Extract screenshots complete');
                    resolve();
                })
                .on('error', (err) => {
                    debug('Extract screenshots failed: %s', err.message);
                    reject(err);
                });
            });
        })
        .then(() => {
            let entries = fs.readdirSync(gifFolder);

            for (let i in entries) {
                debug('Read GIF folder and get file types');
                let buffer = readChunk.sync(path.join(gifFolder, entries[i]), 0, 262),
                    type = fileType(buffer);

                if (!type || type.mime !== 'image/png') {
                    debug('Remove non png file with type: %o', type);
                    fs.unlinkSync(path.join(gifFolder, entries[i]));
                }
            }

            debug('Read GIF folder');
            return Promise.map(fs.readdirSync(gifFolder), (entry) => {
                return new Promise((rs, rj) => {
                    debug('Resize screenshot');
                    sharp(path.join(gifFolder, entry))
                        .resize(320, 180, {
                            kernel: sharp.kernel.cubic,
                            interpolator: sharp.interpolator.nohalo
                        })
                        .crop()
                        .toFile(path.join(gifFolder, 'sc-' + entry), (err) => {
                            fs.unlinkSync(path.join(gifFolder, entry));
                            if (err) {
                                debug('Resize screenshot failed: %s', err.message);
                                rj(err);
                            } else {
                                debug('Resize screenshot complete');
                                rs();
                            }
                        });
                });
            }, {concurrency: 1})
            .then(() => {
                return new Promise((rs, rj) => {
                    debug('Build preview GIF');
                    gm(path.join(gifFolder, '*.png'))
                        .delay(50).colors(64).write(path.join(path.dirname(file), path.basename(file, path.extname(file)) + '.gif'), (err) => {
                        if (err) {
                            debug('Build preview GIF failed: %s', err.message);
                            rj(err);
                        } else {
                            debug('Remove GIF folder');
                            if (fs.existsSync(gifFolder)) {
                                fs.removeSync(gifFolder);
                            }
                            debug('Build preview GIF complete');
                            rs(gifFolder + '.gif');
                        }
                    });
                });
            });
        });
};

parseEntry(config.basePath)
.then(() => {
    if (!config.getFileInfo) {
        return;
    }
    return Promise.map(filesToDo, file => {
        return new Promise((resolve, reject) => {
            const args = `-v quiet -print_format json -show_format -show_streams ${sanitizePath(file)}`.split(' ');
            let stdout = '', stderr = '';
            const probe = spawn(config.ffprobe, args, { shell: true });
            probe.stdout.on('data', (data) => {
                stdout += data.toString();
            });
            probe.stderr.on('data', (data) => {
                stderr += data.toString();
            });
            probe.on('close', (code) => {
                if (code !== 0) {
                    debug(`Error getting file info for ${file} with error: ${stderr}`);
                    return resolve();
                }
                probe.stdin.end();
                let outfile = file.replace(config.basePath, config.outPath);
                outfile = path.join(path.dirname(outfile), path.basename(outfile, path.extname(outfile)) + '.json');
                return fs.ensureDir(path.dirname(outfile))
                    .then(() => {
                        return fs.writeFile(outfile, stdout, err => {
                            if (err) {
                                debug(`Error writing file info to ${outfile} with error: ${stderr}`);
                            }
                            resolve();
                        });
                    });
            });
        });
    }, {concurrency: config.concurrency});
})
.then(() => {
    return Promise.map(filesToDo, file => {
        debug(`Processing ${file}`);
        const extName = path.extname(file),
            baseName = path.basename(file, extName),
            pathName = path.dirname(file.replace(config.basePath, config.outPath));
        let _exists;
        return fs.mkdirp(pathName)
            .then(() => {
                return fs.exists(path.join(pathName, baseName));
            })
            .then(exists => {
                _exists = exists;
                const outfile = path.join(pathName, `${baseName}.webm`);
                if (!_exists && config.encode && config.makeWebm) {
                    return processFile(file, outfile, getWebmCommand(file, outfile))
                        .then(() => {
                            return outfile;
                        });
                }
                return outfile;
            })
            .then(outfile => {
                if (config.s3upload) {
                    return fs.exists(file)
                        .then(exists => {
                            if (!exists) {
                                debug(`Not uploading non-existant file: ${file}`);
                                return;
                            }
                            return makeS3Key(outfile)
                                .then(key => uploadFile(outfile, config.s3bucket, key));
                        });
                }
                if (config.fakes3upload) {
                    return fs.exists(file)
                        .then(exists => {
                            if (!exists) {
                                debug(`Not fake-uploading non-existant file: ${file}`);
                                return;
                            }
                            return makeS3Key(outfile)
                                .then(key => fakeUploadFile(outfile, config.s3bucket, key));
                        });
                }
            })
            .then(() => {
                const outfile = path.join(pathName, `${baseName}.mp4`);
                if (!_exists && config.encode && config.makeMP4) {
                    return processFile(file, outfile, getMP4Command(file, outfile, config.audioCodec))
                        .then(() => {
                            return outfile;
                        });
                }
                return outfile;
            })
            .then(outfile => {
                if (config.makeThumbs) {
                    return getThumbNail(outfile)
                        .then(() => {
                            return outfile;
                        });
                }
                return outfile;
            })
            .then(outfile => {
                if (config.s3upload) {
                    return fs.exists(file)
                        .then(exists => {
                            if (!exists) {
                                debug(`Not uploading non-existant file: ${file}`);
                                return;
                            }
                            return makeS3Key(outfile)
                                .then(key => uploadFile(outfile, config.s3bucket, key));
                        });
                }
                if (config.fakes3upload) {
                    return fs.exists(file)
                        .then(exists => {
                            if (!exists) {
                                debug(`Not fake-uploading non-existant file: ${file}`);
                                return;
                            }
                            return makeS3Key(outfile)
                                .then(key => fakeUploadFile(outfile, config.s3bucket, key));
                        });
                }
            });
    }, {concurrency: config.concurrency});
})
.then(() => {
    return fs.writeFile('errors.json', JSON.stringify(filesErrored));
})
.then(() => {
    console.log('Done.');
    process.exit(0);
})
.catch(err => {
    console.log(`ERROR: ${err.message}`);
    process.exit(err.code);
});