import { app } from 'mu';
import { getUnprocessedPublishedResources, updateStatus, PENDING_STATUS, SUCCESS_STATUS, FAILED_STATUS } from './support/queries';
import { startPipeline } from './support/pipeline';

const PENDING_TIMEOUT = process.env.PENDING_TIMEOUT_HOURS || 3;

/**
 * Starts extracting the published resources.
 */
app.post('/publish-tasks', async function(req, res) {
  try {

    let unprocessedResources = await getUnprocessedPublishedResources(PENDING_TIMEOUT);

    //lock resources (yes yes should be batch operation)
    for(const item of unprocessedResources){
      console.log(`-- Locking resources: ${item.resource}`);
      item.numberOfRetries = (item.numberOfRetries || 0) + 1;
      await updateStatus(item, PENDING_STATUS, );
    }

    for (const item of unprocessedResources) {
      console.log(`-- Start processing: ${item.resource}`);

      try {
        await startPipeline(item);
        await updateStatus(item, SUCCESS_STATUS, item.numberOfRetries);
      }

      catch(e){
        console.log(`--Error processing: ${item.resource}`);
        console.log(e);
        await updateStatus(item, FAILED_STATUS, item.numberOfRetries);
      }
    }

    res.send( { success: true } );

  } catch (err) {
    console.log("We had a bonobo");
    console.log(err);
    res
      .status(500)
      .send( { message: `An error occurred while publishing`,
               err: JSON.stringify(err) } );
  }
} );
