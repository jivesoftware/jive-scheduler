var q = require('q');

exports.testSimpleSingleEvent = function( jive, testUtils, scheduler ) {
    var deferred = q.defer();

    var count = 0;
    jive.events.addDefinitionEventListener( 'event1', 'event1Listener', function() {
        count++;
    });

    jive.service.scheduler().init();
    scheduler.schedule( 'event1', { eventListener: 'event1Listener' } );

    // immediate
    setTimeout( function() {
        if ( count == 1 ) {
            deferred.resolve();
        } else {
            deferred.reject();
        }
    }, 1);

    return deferred.promise;
};

exports.testSimpleIntervalEvent = function( jive, testUtils, scheduler ) {
    var deferred = q.defer();

    var count = 0;
    jive.events.addDefinitionEventListener( 'event1', 'event1Listener', function() {
        count++;
    });

    scheduler.init( jive.events.eventHandlerMap, {}, jive );
    scheduler.schedule( 'event1', { eventListener: 'event1Listener' }, 500 );

    // immediate
    setTimeout( function() {
        if ( count == 2 ) {
            deferred.resolve();
        } else {
            deferred.reject();
        }
    }, 1501);

    return deferred.promise;
};

exports.testSingleEventWithDelay = function( jive, testUtils, scheduler ) {
    var deferred = q.defer();

    var count = 0;
    jive.events.addDefinitionEventListener( 'event1', 'event1Listener', function() {
        count++;
    });

    jive.service.scheduler().init();
    scheduler.schedule( 'event1', { eventListener: 'event1Listener' }, undefined, 500 );

    // immediate
    setTimeout( function() {
        if ( count == 1 ) {
            deferred.resolve();
        } else {
            deferred.reject();
        }
    }, 1501);

    return deferred.promise;
};

exports.testIntervalEventWithDelay = function( jive, testUtils, scheduler ) {
    var deferred = q.defer();

    var count = 0;
    jive.events.addDefinitionEventListener( 'event1', 'event1Listener', function() {
        count++;
    });

    jive.service.scheduler().init();
    scheduler.schedule( 'event1', { eventListener: 'event1Listener' }, 500, 500 );

    // immediate
    setTimeout( function() {
        if ( count == 2 ) {
            deferred.resolve();
        } else {
            deferred.reject();
        }
    }, 2001);

    return deferred.promise;
};

exports.testSingleEventTimeout = function( jive, testUtils, scheduler ) {
    var deferred = q.defer();

    jive.events.addDefinitionEventListener( 'event1', 'event1Listener', function() {
        return q.defer();
    });

    jive.service.scheduler().init();
    scheduler.schedule( 'event1', { eventListener: 'event1Listener' }, undefined, undefined, undefined, 200).then( function() {
        deferred.resolve();
    }, function() {
        deferred.reject();
    });

    return deferred.promise;
};

exports.testIntervalEventTimeout = function( jive, testUtils, scheduler ) {
    var deferred = q.defer();

    var count = 0;
    jive.events.addDefinitionEventListener( 'event1', 'event1Listener', function() {
        count++;
        return q.defer();
    });

    jive.service.scheduler().init();
    scheduler.schedule( 'event1', { eventListener: 'event1Listener' }, undefined, undefined, undefined, 200);

    // immediate
    setTimeout( function() {
        if ( count > 0 ) {
            deferred.resolve();
        } else {
            deferred.reject();
        }
    }, 2001);

    return deferred.promise;
};

exports.testOverlappingIntervalEvents = function( jive, testUtils, scheduler ) {
    var deferred = q.defer();

    var count = 0;
    jive.events.addDefinitionEventListener( 'event1', 'event1Listener', function() {
        var p = q.defer();
        setTimeout( function() {
            count++;
            p.resolve();
        }, 500);

        return p.promise;
    });

    jive.service.scheduler().init();
    scheduler.schedule( 'event1', { eventListener: 'event1Listener' }, 10 );
    scheduler.schedule( 'event1', { eventListener: 'event1Listener' }, 10 );
    scheduler.schedule( 'event1', { eventListener: 'event1Listener' }, 10 );

    // immediate
    setTimeout( function() {
        if ( count == 3 ) {
            deferred.resolve();
        } else {
            deferred.reject();
        }
    }, 2001);

    return deferred.promise;
};

exports.testOverlappingSingleNonExclusiveEvent = function( jive, testUtils, scheduler ) {
    var deferred = q.defer();

    var count = 0;
    jive.events.addDefinitionEventListener( 'event1', 'event1Listener', function() {
        count++;
    });

    jive.service.scheduler().init();
    scheduler.schedule( 'event1', { eventListener: 'event1Listener' } );
    scheduler.schedule( 'event1', { eventListener: 'event1Listener' } );

    // immediate
    setTimeout( function() {
        if ( count == 2 ) {
            deferred.resolve();
        } else {
            deferred.reject();
        }
    }, 1);

    return deferred.promise;
};

exports.testOverlappingSingleExclusiveEvent = function( jive, testUtils, scheduler ) {
    var deferred = q.defer();

    var count = 0;
    jive.events.addDefinitionEventListener( 'event1', 'event1Listener', function() {
        count++;
        return q.resolve();
    });

    jive.service.scheduler().init();
    scheduler.schedule( 'event1', { eventListener: 'event1Listener' }, undefined, undefined, true );
    scheduler.schedule( 'event1', { eventListener: 'event1Listener' }, undefined, undefined, true );

    // immediate
    setTimeout( function() {
        if ( count == 1 ) {
            deferred.resolve();
        } else {
            deferred.reject();
        }
    }, 1);

    return deferred.promise;
};

exports.testConcurrentIntervalEvents = function( jive, testUtils, scheduler ) {
    var deferred = q.defer();

    var count1 = 0;
    jive.events.addDefinitionEventListener( 'event1', 'event1Listener', function() {
        count1++;
        return q.resolve();
    });

    var count2 = 0;
    jive.events.addDefinitionEventListener( 'event2', 'event1Listener', function() {
        count2++;
        return q.resolve();
    });

    jive.service.scheduler().init();
    scheduler.schedule( 'event1', { eventListener: 'event1Listener' }, 500 );
    scheduler.schedule( 'event2', { eventListener: 'event1Listener' }, 500 );

    // immediate
    setTimeout( function() {
        if ( count1 == 2 && count2 == 2 ) {
            deferred.resolve();
        } else {
            deferred.reject();
        }
    }, 1501);

    return deferred.promise;
};

