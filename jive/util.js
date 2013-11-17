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

exports.secondsElapsed = function( timestamp ) {
    return ( new Date().getTime() - timestamp ) / 1000;
};

exports.minutesElapsed = function( timestamp ) {
    return ( ( new Date().getTime() - timestamp ) / 1000 ) / 60;
};

exports.hasSecondsElapsed = function( timestamp, seconds ) {
    return exports.secondsElapsed( timestamp ) > seconds;
};

exports.hasMinutesElapsed = function( timestamp, minutes ) {
    return exports.minutesElapsed( timestamp ) > minutes;
};

exports.sec = function( seconds ) {
    return seconds * 1000;
};

exports.min = function( minutes ) {
    return exports.sec( minutes * 60 );
};

exports.removeJob = function( kueJob ) {
    var deferred = q.defer();
    kueJob.remove(function() {
        jive.logger.debug('job', kueJob.id, kueJob['data']['eventID'], 'expired, removed');
        deferred.resolve();
    });

    return deferred.promise;
};

exports.updateRunning = function( redisClient, eventID ) {
    var deferred = q.defer();
    redisClient.set( eventID + ':liveness', new Date().getTime(), function(err, result) {
        if ( err ) {
            deferred.reject(err);
        } else {
            deferred.resolve(result);
        }
    });

    return deferred.promise;
};

exports.stopRunning = function( redisClient, eventID ) {
    var deferred = q.defer();
    redisClient.del( eventID + ':liveness', function() {
        deferred.resolve();
    });

    return deferred.promise;
};

exports.isRunning = function(redisClient, eventID) {
    var deferred = q.defer();
    redisClient.get( eventID + ':liveness', function(err, result) {
        if (err) {
            deferred.resolve(false);
            return;
        }

        // must be updating liveness at least 5 seconds to be considered 'alive'
        deferred.resolve( !exports.hasSecondsElapsed(result, 5) );
    });

    return deferred.promise;
};

exports.markSuccessfulRun = function(redisClient, eventID ) {
    var deferred = q.defer();

    redisClient.set( eventID + ':lastSuccessfulRun', new Date().getTime(), function(err, result) {
        if ( err ) {
            deferred.reject(err);
        } else {
            deferred.resolve(result);
        }
    });

    return deferred.promise;
};

exports.getLastSuccessfulRun = function(redisClient, eventID ) {
    var deferred = q.defer();

    redisClient.get( eventID + ':lastSuccessfulRun', function(err, result) {
        if ( err ) {
            deferred.reject(0);
        } else {
            deferred.resolve(result);
        }
    });

    return deferred.promise;
};

exports.getQueues = function(redisClient) {
    var deferred = q.defer();
    redisClient.keys("q:jobs:*", function (err, replies) {
        var queues = [];
        replies.forEach(function (reply, i) {
            var parts = reply.split(':');
            if ( parts.length > 3 ) {
                var queueName = parts[2];
                if ( queueName !== 'work' && queues.indexOf(queueName) < 0 ) {
                    queues.push( queueName );
                }
            }
        });

        deferred.resolve( queues );
    });

    return deferred.promise;
};

/**
 * Return all jobs in the given queue that have one of the given states.
 * @param queueName - name of the queue
 * @param types - array of strings containing the job statuses we want (e.g. ['delayed']).
 * @returns a promise for an array of jobs.
 */
exports.searchJobsByQueueAndTypes = function(queueName, types, filter) {
    var deferred = q.defer();
    if (!types) {
        types = ['delayed','active','inactive'];
    }
    if (!types.forEach) {
        types = [types];
    }
    var promises = [];
    types.forEach(function(type) {
        var defer = q.defer();
        promises.push(defer.promise);

        kue.Job.rangeByType(queueName, type, 0, -1, 'asc', function (err, jobs) {
            if (err) {
                jive.logger.error(err);
                process.exit(-1);
            } else {
                if ( filter ) {
                    defer.resolve( filter( jobs, type ) );
                } else {
                    defer.resolve(jobs);
                }
            }
        });

    });
    q.all(promises).then(function(jobArrays) {
        deferred.resolve(jobArrays.reduce(function(prev, curr) {
            return prev.concat(curr);
        }, []));
    }, function(err) {
        deferred.reject(err);
    }).catch( function(e){
            jive.logger.error(e);
        });
    return deferred.promise;
};

