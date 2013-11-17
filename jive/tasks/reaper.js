/*
 * Copyright 2013 Jive Software
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/x    licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

var jive = require('jive-sdk');
var kue = require('kue');
var q = require('q');
var util = require('../util');

exports.taskName = 'jive.reaper';

/**
 * kue specific cleanup job that should run periodically to reap the job result records in redis
 */
exports.execute = function() {
    var deferred = q.defer();
    jive.logger.debug("Running reaper");
    kue.Job.rangeByState('complete', 0, -1, 'asc', function (err, jobs) {
        if ( err) {
            jive.logger.error(err);
            process.exit(-1);
        }
        var promises = [];
        if ( jobs ) {
            jobs.forEach( function(job) {
                var elapsed = util.secondsElapsed( job.created_at );
                if ( elapsed > util.min(10) ) {
                    // if completed more than 10 minutes ago, nuke it
                    promises.push( util.removeJob(job) );
                }
            });
        }

        if ( promises.length > 0 ) {
            q.all(promises).then( function() {
                jive.logger.info("Reaper task cleaned up", promises.length);
            }).finally(function() {
                    deferred.resolve();
                });
        } else {
            jive.logger.info("Cleaned up nothing");
            deferred.resolve();
        }
    });
    return deferred.promise;
};