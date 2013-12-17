var assert = require('assert');
var q = require('q');
var tests = require('../baseSchedulerTest');

describe('jive', function () {

    describe('scheduler', function () {

        it('testSimpleSingleEvent', function (done) {
            var jive = this['jive'];
            var testUtils = this['testUtils'];
            var jiveKue = this['jiveKue'];

            var scheduler = jive.service.scheduler(new jiveKue());
            tests.testSimpleSingleEvent(jive, testUtils, scheduler).then( function() {
                done();
            }, function() {
                assert.fail()
            }).finally( function() {
                scheduler.shutdown();
            });
        });

        it('testSimpleIntervalEvent', function (done) {
            var jive = this['jive'];
            var testUtils = this['testUtils'];

            var scheduler = jive.service.scheduler(new jiveKue());
            tests.testSimpleIntervalEvent(jive, testUtils, scheduler).then( function() {
                done();
            }, function() {
                assert.fail()
            }).finally( function() {
                scheduler.shutdown();
        });
        });

        it('testSingleEventWithDelay', function (done) {
            var jive = this['jive'];
            var testUtils = this['testUtils'];

            var scheduler = jive.service.scheduler(new jiveKue());
            tests.testSingleEventWithDelay(jive, testUtils, scheduler).then( function() {
                done();
            }, function() {
                assert.fail()
            }).finally( function() {
                scheduler.shutdown();
            });
        });

        it('testIntervalEventWithDelay', function (done) {
            var jive = this['jive'];
            var testUtils = this['testUtils'];

            var scheduler = jive.service.scheduler(new jiveKue());
            tests.testIntervalEventWithDelay(jive, testUtils, scheduler).then( function() {
                done();
            }, function() {
                assert.fail()
            }).finally( function() {
                scheduler.shutdown();
            });
        });

        it('testSingleEventTimeout', function (done) {
            var jive = this['jive'];
            var testUtils = this['testUtils'];

            var scheduler = jive.service.scheduler(new jiveKue());
            tests.testSingleEventTimeout(jive, testUtils, scheduler).then( function() {
                done();
            }, function() {
                assert.fail()
            }).finally( function() {
                scheduler.shutdown();
            });
        });

        it('testIntervalEventTimeout', function (done) {
            var jive = this['jive'];
            var testUtils = this['testUtils'];

            var scheduler = jive.service.scheduler(new jiveKue());
            tests.testIntervalEventTimeout(jive, testUtils, scheduler).then( function() {
                done();
            }, function() {
                assert.fail()
            }).finally( function() {
                    scheduler.shutdown();
                });
        });

        it('testOverlappingIntervalEvents', function (done) {
            var jive = this['jive'];
            var testUtils = this['testUtils'];

            var scheduler = jive.service.scheduler(new jiveKue());
            tests.testOverlappingIntervalEvents(jive, testUtils, scheduler).then( function() {
                done();
            }, function() {
                assert.fail()
            }).finally( function() {
                scheduler.shutdown();
            });
        });

        it('testOverlappingSingleNonExclusiveEvent', function (done) {
            var jive = this['jive'];
            var testUtils = this['testUtils'];

            var scheduler = jive.service.scheduler(new jiveKue());
            tests.testOverlappingSingleNonExclusiveEvent(jive, testUtils, scheduler).then( function() {
                done();
            }, function() {
                assert.fail()
            }).finally( function() {
                scheduler.shutdown();
            });
        });

        it('testOverlappingSingleExclusiveEvent', function (done) {
            var jive = this['jive'];
            var testUtils = this['testUtils'];

            var scheduler = jive.service.scheduler(new jiveKue());
            tests.testOverlappingSingleExclusiveEvent(jive, testUtils, scheduler).then( function() {
                done();
            }, function() {
                assert.fail()
            }).finally( function() {
                    scheduler.shutdown();
                });
        });

        it.only('testConcurrentIntervalEvents', function (done) {
            var jive = this['jive'];
            var testUtils = this['testUtils'];

            var scheduler = jive.service.scheduler(new jiveKue());
            tests.testConcurrentIntervalEvents(jive, testUtils, scheduler).then( function() {
                done();
            }, function() {
                assert.fail()
            }).finally( function() {
                scheduler.shutdown();
            });
        });


    });

});

