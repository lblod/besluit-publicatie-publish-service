// eslint-disable-next-line import/no-unresolved
import { app, errorHandler } from "mu";
import { CronJob } from "cron";
// eslint-disable-next-line import/no-unresolved
import bodyParser from "body-parser";
import {
  getUnprocessedPublishedResources,
  updateStatus,
  PENDING_STATUS,
  SUCCESS_STATUS,
  FAILED_STATUS,
} from "./support/queries";
import { startPipeline } from "./support/pipeline";

const PENDING_TIMEOUT = process.env.PENDING_TIMEOUT_HOURS || 3;
const CRON_FREQUENCY = process.env.CACHING_CRON_PATTERN || "0 */5 * * * *";
const MAX_ATTEMPTS = parseInt(process.env.MAX_ATTEMPTS || 10);
const SEARCH_GRAPH =
  process.env.SEARCH_GRAPH || "http://mu.semte.ch/graphs/public";

console.info(`besluit-publicatie-publish-service starting at ${new Date()}`);
console.debug({
  PENDING_TIMEOUT,
  CRON_FREQUENCY,
  MAX_ATTEMPTS,
  SEARCH_GRAPH,
});
async function startPublishing(origin = "http call") {
  console.log(`Service triggered by ${origin} at ${new Date().toISOString()}`);
  const unprocessedResources = await getUnprocessedPublishedResources(
    SEARCH_GRAPH,
    PENDING_TIMEOUT,
    MAX_ATTEMPTS,
  );
  console.log(`Found ${unprocessedResources.length} to process`);

  // lock resources (yes yes should be batch operation)
  for (const item of unprocessedResources) {
    console.log(`-- Locking resources: ${item.resource}`);
    await updateStatus(item, PENDING_STATUS, item.numberOfRetries);
    item.numberOfRetries = parseInt(item.numberOfRetries || 0) + 1;
  }

  for (const item of unprocessedResources) {
    console.log(`Start processing: ${item.resource}`);

    try {
      await startPipeline(item);
      await updateStatus(item, SUCCESS_STATUS, item.numberOfRetries);
    } catch (e) {
      console.log(`Error processing: ${item.resource}`);
      console.log(e);
      await updateStatus(item, FAILED_STATUS, item.numberOfRetries);
    }
  }
}

class PublishingQueue {
  constructor() {
    this.queue = [];
    this.run();
  }

  async run() {
    if (this.queue.length > 0) {
      console.log("executing oldest task on queue");
      try {
        await startPublishing(this.queue.shift());
      } catch (e) {
        const errorMessage = e.message ? e.message : e;
        console.error(`publishing failed: ${errorMessage}`);
        console.info(e);
        // eslint-disable-next-line no-use-before-define
        queue.addJob("queue failure");
      } finally {
        setTimeout(() => {
          this.run();
        }, 500);
      }
    } else {
      setTimeout(() => {
        this.run();
      }, 3000);
    }
  }

  addJob(origin) {
    this.queue.push(origin);
  }
}
const queue = new PublishingQueue();

new CronJob(
  CRON_FREQUENCY,
  async () => {
    try {
      queue.addJob("cron job");
    } catch (err) {
      console.log("We had a bonobo");
      console.log(err);
    }
  },
  null,
  true,
);

// Also parse application/json as json
app.use(
  bodyParser.json({
    type(req) {
      return /^application\/json/.test(req.get("content-type"));
    },
    limit: "500mb",
  }),
);

/**
 * Starts extracting the published resources.
 */
app.post("/publish-tasks", async (req, res) => {
  try {
    queue.addJob("http call");
    res.send({ success: true });
  } catch (err) {
    console.log("We had a bonobo");
    console.log(err);
    res.status(500).send({
      message: `An error occurred while publishing`,
      err: JSON.stringify(err),
    });
  }
});
app.use(errorHandler);
