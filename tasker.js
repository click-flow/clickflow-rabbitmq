const cfpsj = require('./CFPubSubJobs')
cfpsj.enqueueTask('cftask', '{ "thisis": "a test again" }');