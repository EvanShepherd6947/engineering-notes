# Postmortem — a cron-triggered cleanup job, a 15-minute worker limit, duplicate reports

**Short answer:** make the cron trigger do exactly one thing — enqueue a job with a deterministic ID — and let a queue worker that lives outside the 15-minute limit run the cleanup and the reports, checkpointing as it goes. A scheduled trigger is a clock, not a runtime for long-running background jobs, and every alternative I've tried that blurs those two roles has eventually paged me.

I run cron and queue infrastructure for a payments-adjacent platform. Most of what I believe about this came out of a postmortem I had to write about my own design.

## The 03:00 cleanup that ran twice

The job looked harmless for about a year.

It was a nightly sweep: delete expired session rows for every active account, roll up yesterday's usage into a per-account report, hand the report off to the mailer. One scheduled trigger on a managed runner, one process, no queue. At 12,400 accounts it finished in about six minutes, well inside the runner's hard 900-second cap, and nobody thought about it again until we onboarded a batch of larger tenants and the runtime crept past eleven. That's when our internal metrics service — which caps clients at 600 requests per minute — started answering the report step with 429. The retry helper I'd written two years earlier caught it, slept a flat 200 ms, tried three times, and then returned `nil` on exhaustion so the sweep could "keep going". So the sweep kept going. It logged nothing above debug level, skipped roughly 1,800 accounts, and got killed at the 900-second wall with an exit code that our alerting treated as a normal timeout retry.

The next night it started over from scratch. About 10,600 accounts that had already been swept got a second report email, and the phone went off at 03:22 with a customer asking why yesterday's invoice summary arrived twice.

Two things went wrong, and only one of them was the rate limit. The 429 was ordinary — services throttle, that's the contract, and RFC 6585 defines the status precisely so clients can back off. The real defect was that a retry loop turned a throttle into silence, and a single process owned both the schedule and the work, so partial progress had nowhere to live.

## Should a cron trigger run the cleanup job itself, or just enqueue it for a worker?

Enqueue. The trigger's job is to answer one question — has the work for this window been handed off yet — and then get out of the way.

That split buys you something specific: the trigger becomes safe to fire twice. Hosted schedulers are explicitly best-effort about firing time; GitHub's docs say scheduled workflows can be delayed during periods of high load, and I've seen a five-minute delay turn into two overlapping runs when someone also clicked the manual trigger. If the trigger only enqueues, an overlap is harmless as long as the message identity is derived from the window rather than from the clock reading. I use `cleanup-{account}-{YYYY-MM-DDTHH}`. Same window, same ID, one message.

Queues give you a place to enforce that. A FIFO queue on SQS will drop a duplicate carrying the same deduplication ID inside a five-minute interval, and if you run your own queue on Postgres a unique index on the job ID does the same thing for free. Neither one makes your handler idempotent — they just shrink the window in which you need to care.

| Where the schedule runs | Survives a 15-minute cap | Duplicate exposure | What it costs |
| --- | --- | --- | --- |
| In-process timer inside the app | No, it dies with the process | Low until you scale to two instances | Cheapest to build, hardest to observe |
| Platform cron calling an HTTP endpoint that does the work | No | High, retries re-run everything | Simple, but the scheduler owns your runtime |
| Cron trigger that enqueues, worker pool that drains | Yes | Low with deterministic IDs | One more component to monitor |
| Durable workflow engine with checkpointed steps | Yes | Low | Highest operational and learning cost |

Most teams asking this question are on Node.js, and the shape is identical there — my code is Go because our runbooks are Go, but an in-process `node-cron` timer has the same flaw as any other in-process timer: it's a scheduler whose durability is tied to a process that gets redeployed twice a day.

## What the enqueue path owes you

Dispatch is the cheap half, so make it strict. It should be a pure fan-out with no business logic, and it should refuse to exit successfully when it hasn't handed off everything.

```go
// dispatch.go — the scheduled entrypoint. It enqueues, then exits.
// It never runs the cleanup itself.
func Dispatch(ctx context.Context, q Queue, window time.Time) error {
	accounts, err := accountsNeedingCleanup(ctx, window)
	if err != nil {
		return fmt.Errorf("list accounts: %w", err)
	}

	var rejected int
	for _, a := range accounts {
		job := Job{
			// Same account + same hourly window => same ID, so a late trigger,
			// a manual re-run and a retry all collapse onto one message.
			ID:      fmt.Sprintf("cleanup-%s-%s", a.ID, window.UTC().Format("2006-01-02T15")),
			Account: a.ID,
			Window:  window,
		}
		if err := q.Enqueue(ctx, job); err != nil {
			rejected++
			log.Printf("enqueue rejected account=%s err=%v", a.ID, err)
		}
	}
	if rejected > 0 {
		// Loud on purpose: a partial dispatch that exits 0 is how I lost 1,800 accounts.
		return fmt.Errorf("dispatch incomplete: %d of %d jobs not enqueued", rejected, len(accounts))
	}
	return nil
}
```

The retry loop underneath it is where my incident actually lived, so it gets its own rules: honour `Retry-After` when the server sends one, count throttles as a first-class metric, and treat an exhausted loop as an error rather than a shrug.

```go
func (c *Client) Enqueue(ctx context.Context, j Job) error {
	var last error
	for attempt := 0; attempt < 6; attempt++ {
		resp, err := c.post(ctx, j)
		switch {
		case err != nil:
			last = err
		case resp.StatusCode == http.StatusTooManyRequests:
			c.metrics.RateLimited.Inc() // the counter I didn't have on the night it mattered
			last = fmt.Errorf("throttled: %s", resp.Status)
		case resp.StatusCode < 300:
			return nil
		default:
			return fmt.Errorf("enqueue: status %s", resp.Status)
		}
		select {
		case <-time.After(retryAfter(resp, backoff(attempt))): // retryAfter tolerates a nil resp
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	// Never swallow this. An exhausted retry loop is an incident, not a log line.
	return fmt.Errorf("enqueue gave up after 6 attempts: %w", last)
}
```

Test it by running the same window twice against a fake clock and asserting the row count doesn't move. That test is four lines and it would have caught my duplicate reports before a customer did.

## Draining work that outlives a 15-minute limit

A worker isn't magic; it just has a different clock. The cap you're escaping — 900 seconds on a lot of managed function runtimes — gets replaced by a lease you renew, and a queue's visibility timeout, which on SQS can stretch to twelve hours. Neither is an excuse to write a job that runs for six hours in one uninterruptible block. Chunk the work, save a checkpoint after every chunk, and make the chunk itself idempotent so redelivery costs you a wasted batch instead of a duplicate email.

```go
func Run(ctx context.Context, j Job, lease *Lease) error {
	done, err := LoadCheckpoint(ctx, j.ID)
	if err != nil {
		return err
	}
	for _, batch := range Batches(j, done, 500) {
		if err := lease.Renew(ctx, 5*time.Minute); err != nil {
			return fmt.Errorf("lease lost, another worker owns %s: %w", j.ID, err)
		}
		if err := PurgeAndReport(ctx, batch); err != nil {
			return err // redelivery resumes at the last checkpoint, not at zero
		}
		if err := SaveCheckpoint(ctx, j.ID, batch.Last); err != nil {
			return err
		}
	}
	return MarkDone(ctx, j.ID)
}
```

Deployment matters more than people expect here. A rolling deploy in the middle of a four-hour cleanup will kill workers mid-batch, which is fine if `PurgeAndReport` is idempotent and catastrophic if it appends to a report table. I'm not entirely sure there's a general rule for how long a lease should be; ours is five minutes because that's roughly twice our worst observed batch, and your mileage may vary with how spiky your batches are.

Two dashboards earn their keep: oldest-message-age on the queue, and a rate-limited counter per downstream. Queue depth alone lies to you — a queue can look shallow while its oldest message quietly ages out.

## Where this shape is the wrong call

The catch is that you've traded one process for a scheduler, a queue, a worker pool, leases, and checkpoints, and all five need runbooks. For a side project that deletes a few thousand rows a night, stick with the in-process timer and a single-instance deployment; the failure mode is a missed night, which you can fix by hand.

Strict ordering is the other boundary. FIFO queues buy you deduplication and order at a real throughput cost, and a per-account cleanup doesn't support that constraint well — you'd be serialising work that's naturally parallel. If the ordering requirement is genuine, partition it: one FIFO group per account, parallel across accounts.

And if your "job" is a multi-day process with human approvals in the middle, a queue plus checkpoints isn't a good fit — that's a durable workflow engine's problem, and rebuilding one out of cron plus a table is a project, not a weekend.

What's left after all that is a fairly boring rule I now apply everywhere: the schedule decides *when*, the queue decides *what's owed*, the worker decides *how far it got*. Keep those three in separate processes and 03:00 stops being interesting.

## References

- RFC 6585, Additional HTTP Status Codes (429 Too Many Requests): https://www.rfc-editor.org/rfc/rfc6585
- MDN, 429 Too Many Requests and the Retry-After header: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/429
- Amazon SQS FIFO queues, deduplication and ordering: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-fifo-queues.html
- Amazon SQS visibility timeout limits: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
- AWS Lambda function timeout (maximum 900 seconds): https://docs.aws.amazon.com/lambda/latest/dg/configuration-timeout.html
- GitHub Actions events that trigger workflows, including schedule delays: https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows
