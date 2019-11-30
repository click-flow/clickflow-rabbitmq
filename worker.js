const cfpsj = require('./CFPubSubJobs');
const runJob = msg => {
  // this should be where you call your long-runnign
  return Promise.resolve(console.log("This is the job: ", JSON.parse(msg.content.toString())));
};
cfpsj.listen('cftask', runJob);