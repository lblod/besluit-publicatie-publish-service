import { app } from 'mu';
import { getUnprocessedPublishedResources, updateStatus, PENDING_STATUS, SUCCESS_STATUS, FAILED_STATUS } from './support/queries';
import { startPipeline } from './support/pipeline';
import { CronJob } from 'cron';

const PENDING_TIMEOUT = process.env.PENDING_TIMEOUT_HOURS || 3;
const CRON_FREQUENCY = process.env.CACHING_CRON_PATTERN || '0 */5 * * * *';
//TODO: further testing, notulen linken

new CronJob(CRON_FREQUENCY, async function() {
  console.log(`Service triggered by cron job at ${new Date().toISOString()}`);
  try {
    await startPublishing();
  } catch (err) {
    console.log("We had a bonobo");
    console.log(err);
  }
}, null, true);

async function startPublishing(){
  let unprocessedResources = await getUnprocessedPublishedResources(PENDING_TIMEOUT);

  console.log(`Found ${unprocessedResources.length} to process`);

  //lock resources (yes yes should be batch operation)
  for(const item of unprocessedResources){
    console.log(`-- Locking resources: ${item.resource}`);
    item.numberOfRetries = parseInt((item.numberOfRetries || 0)) + 1;
    await updateStatus(item, PENDING_STATUS, );
  }

  for (const item of unprocessedResources) {
    console.log(`Start processing: ${item.resource}`);

    try {
      await startPipeline(item);
      await updateStatus(item, SUCCESS_STATUS, item.numberOfRetries);
    }

    catch(e){
      console.log(`Error processing: ${item.resource}`);
      console.log(e);
      await updateStatus(item, FAILED_STATUS, item.numberOfRetries);
    }
  }
}

/**
 * Starts extracting the published resources.
 */
app.post('/publish-tasks', async function(req, res) {
  try {
    await startPublishing();
    res.send( { success: true } );
  } catch (err) {
    console.log("We had a bonobo");
    console.log(err);
    res
      .status(500)
      .send( { message: `An error occurred while publishing`,
               err: JSON.stringify(err) } );
  }
});
