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

exports.taskName = 'jive.stuck.job.cleaner';

/**
 * kue specific cleanup job that should run periodically to cleanup stuck kue jobs
 */
exports.execute = function(redisClient) {
    var deferred = q.defer();
    jive.logger.debug("Running ", exports.taskName);

    util.getQueues(redisClient).then( function( queues ) {
        queues = queues || [];
        var promises = [];

        queues.forEach( function( queue ) {
            var promise = util.searchJobsByQueueAndTypes(queue, ['active', 'delayed', 'inactive'], stuckJobFilter )
            .then(function(jobs) {
                if (jobs && jobs.length > 0) {
                    return jobs;
                } else {
                    return [];
                }
            });

            promises.push( promise );
        });

        return q.all( promises).then( function() {
           deferred.resolve();
        });
    }).then( function() {
        deferred.resolve();
    });

    return deferred.promise;
};

function stuckJobFilter(jobs, type) {
    var deferred = q.defer();

    if ( type == 'active' ) {
        cleanUpStuckActiveJobs(jobs).then( function(nonStuckJobs) {
            deferred.resolve(nonStuckJobs);
        });
    } else {
        deferred.resolve(jobs);
    }

    return deferred.promise;
}

function cleanUpStuckActiveJobs(activeJobs) {
    var deferred = q.defer();
    var nonStuckJobs = [];
    var promises = [];
    activeJobs.forEach(function(job) {
        if ( util.hasSecondsElapsed( job.updated_at, 20) ) {
            // jobs shouldn't be inactive for more than 20 seconds
            promises.push(failJob(job));
        } else {
            nonStuckJobs.push(job);
        }
    });

    q.all(promises).finally( function() {
        deferred.resolve(nonStuckJobs);
    });

    return deferred.promise;
}

function failJob( job ) {
    job.complete();
    if ( util.hasSecondsElapsed(job.created_at, 10 * 60) ) {
        // try to destroy now, more than 10 minutes old
        return removeJob(job);
    }

    jive.logger.debug('job', job.id, job['data']['eventID'], 'expired, marked complete');
    return q.resolve();
}

function removeJob( job ) {
    var deferred = q.defer();
    job.remove(function() {
        jive.logger.debug('job', job.id, job['data']['eventID'], 'expired, removed');
        deferred.resolve();
    });

    return deferred.promise;
}
