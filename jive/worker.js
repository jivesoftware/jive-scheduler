/*
 * Copyright 2013 Jive Software
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

/**
 * This is a generic worker implementation.
 * You tell it what events it can respond to by passing an event - handler map
 * to its init method.
 * It can subscribe to many queues.
 */

var q = require('q');
var kue = require('kue');
var redis = require('redis');
var jive = require('jive-sdk');

function Worker() {
}

var redisClient;
var jobs;
var eventHandlers;
var scheduler;
var queueName;

///////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////
// public

module.exports = Worker;

Worker.prototype.makeRedisClient = function(options) {
    if (options && options['redisLocation'] && options['redisPort']) {
        return redis.createClient(options['redisPort'], options['redisLocation']);
    }

    return redis.createClient();
};

Worker.prototype.init = function init(_scheduler, handlers, options) {
    scheduler = _scheduler;
    eventHandlers = handlers;
    queueName = options['queueName'];
    var self = this;
    kue.redis.createClient = function() {
        return self.makeRedisClient(options);
    };
    redisClient = self.makeRedisClient(options);
    jobs = kue.createQueue();
    jobs.promote(1000);

    var addQueueListener = function(eventQueueName) {
        jive.logger.info('Subscribing to Redis event: ', eventQueueName);
        jobs.process(eventQueueName, options['concurrentJobs'] || 1000, eventExecutor);
    };

    // analyze event listeners and listen on redis queue for events on them
    for (var eventListener in eventHandlers) {
        if (eventHandlers.hasOwnProperty(eventListener)) {
            var listeners = eventHandlers[eventListener];
            if ( typeof listeners === 'function' ) {
                addQueueListener(queueName + '.' + eventListener);
            } else if (typeof listeners === 'object' ) {
                if ( typeof listeners['forEach'] === 'function' ) {
                    // its really an array
                    addQueueListener(queueName + '.' + eventListener );
                } else {
                    for ( var eventID in listeners ) {
                        if ( listeners.hasOwnProperty(eventID) ) {
                            addQueueListener(queueName + '.' + eventListener + '.' + eventID);
                        }
                    }
                }
            }
        }
    }

    // also listen to anonymous system tasks
    addQueueListener( queueName + '.' + '__jive_system_tasks' );
};

///////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////
// helpers

/**
 * Search the redis queue for the job, and if isn't currently among the active or
 */
function shouldRun( jobMetadata ) {

    var eventID = jobMetadata['eventID'];
    var jobID = jobMetadata['jobID'];
    var exclusive = jobMetadata['exclusive'];

    if (!exclusive ) {
        return q.resolve(true);
    }

    return scheduler.searchTasks(jobMetadata, ['delayed','active','inactive']).then( function(tasks) {
        var runningJob;

        for ( var i = 0; i < tasks.length; i++ ) {
            var task = tasks[i];
            if ( task['data']['jobID'] !== jobID  ) {
                runningJob = task['data']['jobID'] + " : " + task['data']['eventID'];
                break;
            }

        }

        return !runningJob;
    } );
}

/**
 * run the job we took off the work queue
 */
function eventExecutor(job, done) {
    var meta = job.data;

    shouldRun(meta).then( function(shouldRun) {

        var context = meta['context'];
        var eventID = meta['eventID'];
        var eventListener = context['eventListener'];

        if ( !shouldRun ) {
            jive.logger.debug("Execution aborted: " + JSON.stringify(meta,4 ));
            done();
            return;
        }

        var next = function() {
            if ( liveNess ) {
                clearTimeout(liveNess);
            }
            redisClient.set( eventID + ':lastSuccessfulRun', new Date().getTime(), function() {
                done();
            });
        };

        var abort = function( ){
            if ( liveNess ) {
                clearTimeout(liveNess);
            }
            done();
        };

        var liveNess = setInterval( function() {
            // update the job every 1 seconds to ensure liveness
            job.update();
        }, 1000);

        var handlers;
        if (eventListener) {
            var tileEventHandlers = eventHandlers[eventListener];
            if ( !tileEventHandlers ) {
                jive.logger.error("No event handler for " + eventListener + ", eventID " + eventID);
                abort();
                return;
            }
            handlers = tileEventHandlers[eventID];
        } else {
            handlers = eventHandlers[eventID];
        }

        if ( !handlers ) {
            // could find no handlers for the eventID; we're done
            abort();
            return;
        }

        if ( typeof handlers === 'function' ) {
            // normalize single handler into an array
            handlers = [ handlers ];
        }

        var promises = [];
        handlers.forEach( function(handler) {
            try {
                var result = handler(context);
            } catch (e) {
                result = q.reject( e );
            }

            if ( result && result['then'] ) {
                // its a promise
                promises.push( result );
            }
        });

        if ( promises.length > 0 ) {
            q.all( promises ).then(
                // success
                function(result) {
                    if (result) {
                        // if just one result, don't bother storing an array
                        result = result['forEach'] && result.length == 1 ? result[0] : result;
                        job.data['result'] = { 'result' : result };
                        job.update( function() {
                            next();
                        });
                    } else {
                        next();
                    }
                },

                // error
                function(err) {
                    jive.logger.error("Error!", err);
                    job.data['result'] = { 'err' : err };
                    job.update( function() {
                        next();
                    });
                }
            ).done();
        } else {
            next();
        }
    });
}
