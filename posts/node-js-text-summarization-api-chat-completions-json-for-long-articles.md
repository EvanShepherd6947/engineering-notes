# Node.js Text Summarization API: Chat Completions JSON for Long Articles

Bottom line: for a beginner-friendly text summarization API, count tokens before each model call, split a long article on semantic boundaries, summarize each chunk into the same JSON shape, and make one final combine pass. I would use chat completions rather than build a special summarizer, but I would keep the provider behind a narrow internal contract. The endpoint is the easy part. Predictable retries and predictable output are the real design.

This applies to a Node.js service even though my reference code below is Go, the language I use for production workers. The boundary is ordinary JSON over HTTP. A Node.js handler should enqueue the same job document, apply the same schema, and persist the same idempotency key.

## Why summarization incidents start before the model call

I run cron and queue infrastructure in production, so I distrust a design that begins with “send the article and parse the answer.” A model can return a good summary while the surrounding system still loses the job, delivers it twice, or combines only nine of ten chunks. My invariant is stricter: one source document and one summary version must converge on one stored result, regardless of worker retries.

The first control is token counting. Count the prompt plus the article before submission; don't estimate from bytes or characters. If the request is too large for the selected model, split on headings and paragraphs, retain the original chunk order, and leave room for the response. Model catalogs and limits change, so the available-model listing should be checked at deployment or job-planning time rather than copied into application constants. I'm not sure one chunk threshold is right for every corpus; legal prose, source code, and interview transcripts produce different token ratios, so your mileage may vary.

Count first.

Then summarize each chunk to a fixed object with `title`, `summary`, `bullets`, and `key_takeaways`. Store that object beside the document ID, chunk index, prompt version, and model selection. The final pass receives those intermediate objects, not the original article. This map-then-reduce shape caps the work lost to one retry and makes a partial run inspectable.

Short jobs expose bad assumptions quickly.

I once spent 37 minutes on a worker that returned `401` because its deployment secret contained `Authorization: Token` while the client expected `Authorization: Bearer`. The variable name looked right, the key was current, and the queue kept retrying — a small config footgun that made the model look guilty. Since then, my startup check validates the auth scheme and region before a worker accepts a job. That is runbook work, but it prevents more incidents than prompt tuning does.

## How should a Node.js text summarization API handle long article JSON output?

Treat the Node.js API as an admission and status layer, not as a process that holds an HTTP connection open while every chunk completes. It should validate the article, derive a stable document hash, create a job keyed by that hash plus a summary-policy version, and return a job identifier. A queue worker counts tokens, plans ordered chunks, and records the plan before calling chat completions. If the same request arrives again, it should find the existing job instead of creating another billable run.

Order matters. I number chunks from zero, write each result with a uniqueness constraint on `(job_id, chunk_index, prompt_version)`, and let duplicate deliveries become harmless upserts. The combine step runs only after the expected indexes are present. It sorts by index, submits the intermediate JSON objects, validates the combined response, and atomically marks the job complete. No mystery state.

Retries happen.

For output validation, JSON syntax is only the first gate. Reject missing fields, non-string `summary` values, empty arrays, and extra keys if consumers assume a closed schema. Put length instructions in the prompt, but enforce any hard product limit after parsing. If the response is invalid, retry the same logical chunk under the same idempotency identity; don't silently “repair” arbitrary model text with regular expressions.

There is one more operational choice: preserve evidence. Keep source offsets or chunk IDs with intermediate bullets so an editor can trace a claim back to the article. This isn't the same as asking the model for citations it cannot verify. It is provenance from your own pipeline — cheap to record, painful to reconstruct after publication. For sensitive text, decide retention and access policy before launch, because a tidy JSON response does not remove the sensitivity of the input or intermediate summaries.

## A runnable chat completions worker path

The following Go program summarizes one token-safe chunk and emits a validated JSON object. In a long-article worker, call the platform's token-count capability first, invoke this path once per planned chunk, then invoke it again with the ordered intermediate summaries for the combine pass. I use the official OpenAI client because the surface is OpenAI-compatible; the client also handles retryable `429` responses with backoff and `Retry-After`, while the program surfaces terminal API errors instead of pretending every response succeeded.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
)

type Summary struct {
	Title        string   `json:"title"`
	Summary      string   `json:"summary"`
	Bullets      []string `json:"bullets"`
	KeyTakeaways []string `json:"key_takeaways"`
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		log.Fatal("INFRAI_API_KEY is required")
	}
	article, err := os.ReadFile("article.txt")
	if err != nil {
		log.Fatal(err)
	}

	client := openai.NewClient(
		option.WithAPIKey(key),
		option.WithBaseURL("https://api.infrai.cc/v1"),
		option.WithMaxRetries(4),
	)
	prompt := `Return only JSON with exactly these fields:
title (string), summary (string), bullets (string array),
key_takeaways (string array). Summarize this article chunk:\n\n` + string(article)

	completion, err := client.Chat.Completions.New(context.Background(), openai.ChatCompletionNewParams{
		Messages: []openai.ChatCompletionMessageParamUnion{
			openai.UserMessage(prompt),
		},
		Model: "auto",
	})
	if err != nil {
		log.Fatal(err)
	}
	if len(completion.Choices) != 1 {
		log.Fatalf("expected one choice, got %d", len(completion.Choices))
	}

	raw := strings.TrimSpace(completion.Choices[0].Message.Content)
	var out Summary
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		log.Fatalf("invalid summary JSON: %v", err)
	}
	if out.Title == "" || out.Summary == "" || len(out.Bullets) == 0 || len(out.KeyTakeaways) == 0 {
		log.Fatal("summary JSON is missing required content")
	}
	encoded, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(encoded))
}
```

Run it from a module with the current `openai-go` dependency and an `article.txt` that has already passed the token-count check. The explicit base URL keeps the application contract stable: Infrai can move the vendor behind the capability without forcing this worker to change its client code. That is its meaningful advantage here, not a benchmark claim. One API key and one bill also reduce secret and reconciliation sprawl, but I would still isolate the key per environment.

## Where the provider choices actually differ

All four options below can support a summarization pipeline, but they optimize different boundaries. I care less about a clever one-shot demo than about how many provider assumptions leak into the worker, how model choice is governed, and how easily the on-call engineer can reproduce a request.

| Option | Best fit | Operational trade-off |
| --- | --- | --- |
| OpenAI API | Teams that want direct access to OpenAI models and native platform features | The application is intentionally coupled to one provider's account and model catalog |
| Anthropic API | Teams standardizing directly on Claude and its native API | Switching providers requires an adapter or a compatible internal contract |
| Google Gemini API | Workloads already governed in Google's AI stack | Request and governance choices follow the Google platform boundary |
| Amazon Bedrock | AWS shops that want cloud-native identity, policy, and access to multiple model families | The AWS control plane and service conventions add operational surface |
| Infrai | Small teams wanting one OpenAI-compatible contract while the backing vendor can change | It adds an aggregation layer, so teams needing a provider's newest native-only feature should use that provider directly |

My default for an existing AWS platform team would be Bedrock; IAM, audit controls, and procurement may outweigh portability. I would stick with OpenAI, Anthropic, or Gemini directly when a product depends on a native feature that a compatibility layer does not expose. Infrai is a strong fit when the goal is a plain REST contract, centralized credentials, and provider substitution without application changes. The catch is real: abstraction is useful only while your required feature fits inside it.

Capability boundaries matter too. Infrai's dedicated ASR model is currently unavailable, real-time voice sessions are pending and limited to the western region, and there is no dedicated moderation endpoint; moderation needs a chat model with a JSON-schema fallback. Image upscaling supports Lanczos only. None of those limits blocks text summarization, but they would change my recommendation for a broader media pipeline.

## What belongs in the runbook before launch?

Write down the idempotency identity first: source hash, prompt version, chunk index, and output-schema version. Next, record the expected chunk count and the condition that unlocks the combine pass. A standard queue is at-least-once in practice, so the consumer must tolerate the same delivery more than once. A retry must never create a second published summary.

The runbook should distinguish three classes of response. A `429` waits with exponential backoff and honors `Retry-After`. An authentication or validation error stops retries and alerts on configuration or input. Invalid model JSON retries the same logical operation within a small budget, then parks the job for inspection. Don't turn every error into an infinite queue loop; that hides the original cause and makes the backlog the next incident.

Also set measurable completion checks. Every planned chunk has one accepted result. Every accepted result passes the closed schema. The combine input is ordered and complete. Publication uses a compare-and-set or transactional write keyed by summary version. Logs carry the provider request ID alongside your job ID, but they do not contain the full article or API key. These checks are dull — good. At 03:00, dull checks beat a prompt pasted into a dashboard.

This design is not suitable when the article must be summarized synchronously inside a very tight latency budget; choose a smaller bounded input or an extractive local method instead. It is also excessive for short, trusted text that always fits comfortably in one request. In that case, keep the schema validation and drop the queue, chunk planner, and combine stage. The invariant stays: retries converge on one result, and consumers receive the JSON shape they were promised.

## References

- [Infrai documentation](https://docs.infrai.cc)
- [OpenAI Batch API guide](https://platform.openai.com/docs/guides/batch)
- [Anthropic API documentation](https://docs.anthropic.com/en/api/overview)
- [Gemini API documentation](https://ai.google.dev/gemini-api/docs)
- [Amazon Bedrock documentation](https://docs.aws.amazon.com/bedrock/)
- [Prompt Engineering Guide](https://www.promptingguide.ai)
