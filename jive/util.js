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

exports.hasSecondsElapsed = function( timestamp, seconds ) {
    return exports.secondsElapsed( timestamp ) > seconds;
};

exports.sec = function( seconds ) {
    return seconds * 1000;
};

exports.removeJob = function( kueJob ) {
    var deferred = q.defer();
    kueJob.remove(function() {
        jive.logger.debug('job', kueJob.id, kueJob['data']['eventID'], 'expired, removed');
        deferred.resolve();
    });

    return deferred.promise;
};

