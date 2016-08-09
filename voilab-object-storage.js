/*jslint node: true */
(function () {
    'use strict';

    module.exports = function (config) {
        var pkgcloud = require('pkgcloud'),
            fs = require('fs'),
            bs = require('streamifier'),

            client = pkgcloud.storage.createClient({
                provider: config.provider,
                username: config.username,
                password: config.password,
                authUrl: config.authUrl,
                region: config.region
            }),

            getUploadStream = function (remote, cb) {
                var stream = client.upload({
                    container: config.container,
                    remote: remote
                });

                stream.on('error', function (err) {
                    cb(err);
                });

                stream.on('success', function (file) {
                    // ensure staticUrl ends with /
                    if (config.staticUrl.slice(-1) !== '/') {
                        config.staticUrl = config.staticUrl + '/';
                    }
                    cb(null, config.staticUrl + file.name);
                });

                return stream;
            },

            os = {};

        /**
         * Upload a file on the object storage
         *
         * @param  {String}   source   File source URL
         * @param  {String}   remote   File name when uploaded on the object storage
         * @param  {Function} callback
         * @param  {Function} progress
         */
        os.uploadFromUri = function (source, remote, callback, progress) {
            var http = require('http'),

                writeStream = getUploadStream(remote, callback),

                req = http.get(source, function (res) {
                    res.on('error', function (err) {
                        callback(err);
                    });

                    if (progress) {
                        res.on('data', function (chunk) {
                            progress(req, res, chunk);
                        });
                    }

                    res.pipe(writeStream);
                });
            req.on('error', function (err) {
                callback(err);
            });
        };

        /**
         * Upload un fichier depuis une source déposée sur le serveur vers
         * le cloud
         *
         * @param {String}   path     File path
         * @param {String}   remote   File name when uploaded on the object storage
         * @param {Boolean}  unlink   Set true to delete source file after upload
         * @param {Function} cb
         * @returns {void}
         */
        os.uploadFromFile = function (path, remote, unlink, cb) {
            // create a stream from path
            var fstream = fs.createReadStream(path);

            // send it to the cloud
            fstream.pipe(getUploadStream(remote, function (err, url) {
                if (err) {
                    return cb(err);
                }
                // delete file if asked
                if (unlink !== false) {
                    fs.unlink(path, function (err) {
                        if (err) {
                            return cb(err, url);
                        }
                        cb(null, url);
                    });
                } else {
                    cb(null, url);
                }
            }));
        };

        /**
         * Upload a file from a buffer source
         *
         * @param {Buffer} buffer  Buffer to send to the object storage
         * @param {String} remote  File name when uploaded on the object storage
         * @param {Function} cb
         * @returns {void}
         */
        os.uploadFromBuffer = function (buffer, remote, cb) {
            // create a stream from buffer
            var bstream = bs.createReadStream(buffer);

            // send it to the cloud
            bstream.pipe(getUploadStream(remote, cb));
        };

        /**
         * Alias for retro-compatibility
         *
         * @type {os.uploadFromUri|*}
         */
        os.upload = os.uploadFromUri;

        os.getFiles = function (cb) {
            client.getFiles(config.container, cb);
        };

        os.getFile = function (filename, cb) {
            client.getFile(config.container, filename, cb);
        };

        return os;
    };
}());
