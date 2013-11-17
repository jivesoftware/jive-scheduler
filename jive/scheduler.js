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
var worker = require('./worker');
var redis = require('redis');

var jobs;
var redisClient;

var jobQueueName = 'work';
var pushQueueName = 'push';
var scheduleLocalTasks = false;
var localTasks = {};
var reaper = require('./tasks/reaper');
var cleanupStuckJobs = require('./tasks/cleanupStuckJobs');
var util = require('./util');

function Scheduler(options) {
}

module.exports = Scheduler;

///////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////
// public

/**
 * Initialize the scheduler
 * @param _eventHandlerMap - an object that translates eventIDs into functions to run
 * @param serviceConfig - configuration options such as the location of the redis server.
 */
Scheduler.prototype.init = function init( _eventHandlerMap, serviceConfig ) {
    eventHandlerMap = _eventHandlerMap || jive.events.eventHandlerMap;

    var self = this;
    var isWorker  = !serviceConfig || !serviceConfig['role'] || serviceConfig['role'] === jive.constants.roles.WORKER;
    var isPusher  = !serviceConfig || !serviceConfig['role'] || serviceConfig['role'] === jive.constants.roles.PUSHER;

    var opts = setupKue(serviceConfig);
    if (!(isPusher || isWorker)) {
        // schedule no workers to listen on queued events if neither pusher nor worker
        return;
    }

    setupCleanupTasks(eventHandlerMap);

    if ( isWorker ) {
        opts['queueName'] = jobQueueName;
        new worker().init(this, eventHandlerMap, opts);
    }

    if ( isPusher ) {
        opts['queueName'] = pushQueueName;
        new worker().init(this, eventHandlerMap, opts);
    }

    scheduleLocalTasks = isWorker;

    // setup listeners
    jive.events.globalEvents.forEach( function(event) {
        jive.events.addLocalEventListener( event, function(context ) {
            self.schedule( event, context );
        });
    });

    // schedule periodic tasks
    self.schedule(reaper.taskName, {}, util.min(5) );
    self.schedule(cleanupStuckJobs.taskName, {}, util.min(5) );

    jive.logger.info("Redis Scheduler Initialized for queue");
};

/**
 * Schedule a task.
 * @param eventID the named event to fire to perform this task
 * @param context arguments to provide to the event handler
 * @param interval optional, time until this event should be fired again
 *
 * Returns a promise that gets invoked when the scheduled task has completed execution
 * only if its not a recurrent task
 */
Scheduler.prototype.schedule = function schedule(eventID, context, interval, delay, exclusive, timeout) {
    var self = this;
    context = context || {};

    if ( interval ) {
        // if there is an interval, try to execute this task periodically
        // it will only fire if it isn't already running somewhere
        scheduleLocalRecurrentTask(delay, self, eventID, context, interval, timeout);
        return q.resolve();
    }

    var deferred = q.defer();

    var meta = {
        'jobID'     : jive.util.guid(),
        'eventID'   : eventID,
        'context'   : context,
        'interval'  : interval,
        'delay'     : delay,
        'timeout'   : timeout,
        'exclusive' : exclusive
    };

    var job = jobs.create(queueFor(meta), meta);
    if ( interval || delay ) {
        job.delay(interval && !delay ? interval : delay);
    }

    var timeoutWatcher = setTimeout( function() {
        jive.logger.warn("Failed jobID " + meta['jobID'] + " eventID " + eventID + " due to timeout");
        job.failed();

        deferred.resolve();
    }, timeout || util.sec(60) );

    job.on('complete', function() {
        // once the job is done, retrieve any results that were cached on redis by some worker
        // then resolve or reject the promise accordingly.

        clearTimeout( timeoutWatcher );

        kue.Job.get( job.id, function( err, latestJobState ) {

            if ( !latestJobState ) {
                deferred.resolve();
                return;
            }

            var jobResult = latestJobState['data']['result'];
            if ( !err ) {
                deferred.resolve( jobResult ? jobResult['result'] : null );
            } else {
                if ( !jobResult ) {
                    deferred.resolve();
                } else {
                    var parsed = JSON.parse(jobResult);
                    if ( parsed['err'] ) {
                        deferred.reject(  parsed['err'] );
                    } else {
                        deferred.resolve( parsed['result']);
                    }
                }
            }

        });
    });

    jive.logger.debug("Scheduled task: " + eventID, interval || '(no interval)');
    job.save();

    return deferred.promise;
};

/**
 * Remove the task from the queue.
 * @param eventID
 */
Scheduler.prototype.unschedule = function unschedule(eventID){
    clearInterval(localTasks[eventID]);

    this.getTasks().forEach(function(job) {
        if (job.data['eventID'] == eventID) {
            job.remove();
        }
    });
};

/**
 * Returns a promise which resolves with the jobs currently scheduled (recurrent or dormant)
 */
Scheduler.prototype.getTasks = function getTasks() {
    return util.getQueues().then( function( queues ) {
        queues = queues || [];
        var promises = [];

        queues.forEach( function( queue ) {
            var promise = util.searchJobsByQueueAndTypes(queue).then(function(jobs) {
                if (jobs && jobs.length > 0) {
                    return jobs;
                } else {
                    return [];
                }
            });

            promises.push( promise );
        });

        return q.all( promises).then( function(allJobs ) {
            var jobsToReturn = [];
            allJobs.forEach( function(jobset) {
                jobsToReturn = jobsToReturn.concat( jobset );
            });

            return jobsToReturn;
        });
    });
};

Scheduler.prototype.shutdown = function(){
    var scheduler = this;
    this.getTasks().forEach(function(job){
        scheduler.unschedule(job);
    });
};

///////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////
// private helpers

function queueFor(meta) {
    var eventID = meta['eventID'];

    var queueName;
    if (jive.events.pushQueueEvents.indexOf(eventID) != -1 ) {
        queueName = pushQueueName;
    } else {
        queueName = jobQueueName;
    }

    var eventListener;
    if ( meta['context'] ) {
        eventListener = meta['context']['eventListener'];
    }

    if ( eventListener === '__jive_system_tasks' ) {
        return queueName + '.' + eventListener;
    }

    queueName = queueName + ( eventListener ? ('.' + eventListener) : '' ) + '.' + eventID;
    return queueName;
}

function setupKue(options) {
    options = options || {};
    //set up kue, optionally with a custom redis location.
    redisClient = new worker().makeRedisClient(options);
    kue.redis.createClient = function() {
        return new worker().makeRedisClient(options);
    };
    jobs = kue.createQueue();
    jobs.promote(1000);
    return options;
}

/**
 * Recurrent tasks (eg. those with an interval) are fired on a recurrent basis locally.
 * If a task should run, then it gets scheduled for a one time execution by some node in the
 * cluster (could be this same node).
 *
 * The criteria for should run is as follows:
 * - The event has never successfully run
 * - The amount of time elapsed between now and last successful run exceeds the task interval
 * - The task is not already running somewhere else
 */
function scheduleLocalRecurrentTask(delay, self, eventID, context, interval, timeout) {
    if ( !scheduleLocalTasks ) {
        return;
    }

    if ( localTasks[eventID] ) {
        jive.logger.debug("Event", eventID, "already scheduled, skipping.");
        return;
    }

    // evaluate the event last succcessful run time; if its before interval is up
    // then prevent locally scheduled job from being scheduled
    var execute = function() {
        util.getLastSuccessfulRun(redisClient, eventID).then( function(result) {
            var elapsed = (new Date().getTime()) - result;  // in millseconds
            if ( elapsed >= interval ) {
                util.isRunning(redisClient, eventID).then(function (running) {
                    if (!running) {
                        jive.logger.info('scheduling', eventID);
                        var schedule = self.schedule(eventID, context, undefined, undefined, true, timeout);
                        if ( schedule ) {
                            schedule.then( function(result) {
                                jive.logger.debug('job', eventID, 'done', (result ? result : '' ));
                            }, function(err) {
                                jive.logger.debug('job ' + eventID, 'failed: ', err );
                            }).finally( function() {
                                setTimeout( execute, interval );
                            });
                        } else {
                            setTimeout( execute, interval );
                        }
                    } else {
                        jive.logger.debug("Skipping schedule of " + eventID, " - Already running");
                        setTimeout( execute, interval );
                    }
                });
            } else {
                setTimeout( execute, interval );
            }
        });
    };

    setTimeout(function () {
        execute();
    }, delay || interval || 1);

    localTasks[eventID] = true;
}

function setupCleanupTasks(eventHandlerMap) {
    eventHandlerMap[reaper.taskName] = reaper.execute;
    eventHandlerMap[cleanupStuckJobs.taskName] = function() {
        cleanupStuckJobs.execute(redisClient);
    }
}