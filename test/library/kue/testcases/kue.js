var assert = require('assert');
var test = require('../basePersistenceTest');

describe('jive', function () {

    describe ('#persistence.mongo', function () {

        it('save', function (done) {
            var jive = this['jive'];
            var testUtils = this['testUtils'];
            var persistence = this['persistence'];

            test.testSave(testUtils, persistence).then(
                function() {
                    setTimeout( function() {
                        done();
                    }, 1000);
                },

                function(e) {
                    assert.fail(e);
                }
            ).finally( function() {
                    return persistence.close();
                });
        });

        it('find', function (done) {
            var jive = this['jive'];
            var testUtils = this['testUtils'];
            var persistence = this['persistence'];

            test.testFind(testUtils, persistence).then(
                function() {
                    setTimeout( function() {
                        done();
                    }, 1000);
                },

                function(e) {
                    assert.fail(e);
                }
            ).finally( function() {
                    return persistence.close();
                });
        });

        it('remove', function (done) {
            var jive = this['jive'];
            var testUtils = this['testUtils'];
            var persistence = this['persistence'];

            test.testRemove(testUtils, persistence).then(
                function() {
                    done();
                },

                function(e) {
                    assert.fail(e);
                }
            ).finally( function() {
                    return persistence.close();
                });
        });

        // xxx todo

    });

});

