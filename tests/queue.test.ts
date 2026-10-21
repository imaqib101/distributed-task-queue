import { strict as assert } from 'assert';
import { TaskQueue } from '../src/index.js';

async function runTests() {
  const tq = new TaskQueue();
  tq.enqueue({ id: '1', payload: { data: 'test' }, retries: 0, maxRetries: 1 });

  await tq.processNext(async () => {
    throw new Error('Failed processing');
  });

  const metrics = tq.dlq.getMetrics();
  assert.equal(metrics.totalDead, 1);
  console.log('Distributed Task Queue DLQ metrics tests passed!');
}

runTests().catch(err => {
  console.error(err);
  process.exit(1);
});
