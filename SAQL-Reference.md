# Query reference · Campus concert ticket rush

Use this page when you want to change a query or try the optional challenge. The examples use the same input, output, and six request fields as the [student walkthrough](README.md#3-investigate-the-recording).

## Read a query

| Clause or expression | Question it answers in this lab |
| --- | --- |
| `FROM [ticket-requests]` | Which connection supplies the ticket requests? |
| `TIMESTAMP BY eventTime` | Which clock places a request in a window? Here it is the UTC time recorded in the request. By default, an Event Hub input uses enqueue time. |
| `WHERE path = '/checkout'` | Which individual requests enter the calculation? |
| `GROUP BY deviceType, TumblingWindow(second, 10)` | Which requests should be counted together? Here each device type gets its own count in each ten-second interval. |
| `COUNT(*)` | How many matching requests are in this group? |
| `SUM(CASE WHEN statusCode >= 500 THEN 1 ELSE 0 END)` | How many of the requests in this group were server errors? The success rows still contribute to the total request count. |
| `HAVING COUNT(*) >= 3` | Which groups have enough matching requests to report? |
| `System.Timestamp()` | What time should label this window’s result? It returns the window end in these grouped queries. |
| `SELECT … INTO [ticket-results]` | Which columns should the job write, and to which output? |

**Example.** If you need both total traffic and failures, keep all requests and use the conditional sum. If you first add `WHERE statusCode >= 500`, successes are removed before counting and `COUNT(*)` now means “failed requests.”

## Pick a window

| Window | When a report is produced | Ticketing question |
| --- | --- | --- |
| `TumblingWindow(second, 10)` | At each ten-second boundary for groups containing requests. Each request belongs to one window. | How many requests and failures did we have in each reporting interval? |
| `SlidingWindow(second, 10)` | When a request enters or leaves the ten-second lookback and changes its contents. A `HAVING` clause can suppress rows below the alert threshold. | At what instant did we reach three failed checkouts within ten seconds? |
| `HoppingWindow(second, 10, 5)` | Every five seconds for groups with requests in the previous ten seconds. Adjacent windows overlap. | How many failed checkouts should we show if the screen refreshes every five seconds? |

By default, a window includes its end and excludes its start. For a window ending at `19:00:10` and lasting ten seconds, a request at `19:00:10` is included. One at `19:00:00` belongs to the preceding tumbling window. [Microsoft’s windowing overview](https://learn.microsoft.com/en-us/azure/stream-analytics/stream-analytics-window-functions) · [Hopping-window boundaries](https://learn.microsoft.com/en-us/stream-analytics-query/hopping-window-azure-stream-analytics)

## Overlapping reports

The ticketing team wants a checkout failure count refreshed **every five seconds**, using the **previous ten seconds** of data. Return to the stopped job’s query editor with the original sample selected. Predict which reports will contain request `r21`, the phone checkout failure at `19:00:07`.

```sql
SELECT System.Timestamp() AS windowEnd, deviceType, COUNT(*) AS failedCheckouts
INTO [ticket-results]
FROM [ticket-requests] TIMESTAMP BY eventTime
WHERE path = '/checkout' AND statusCode >= 500
GROUP BY deviceType, HoppingWindow(second, 10, 5)
```

Select **Test query**. Compare the result with Query 3 in the walkthrough. Could the team add these counts to get the number of failed requests?

<details>
<summary>Check the overlapping reports</summary>

| Window end (UTC) | Phone checkout failures | Request IDs included |
| --- | ---: | --- |
| 19:00:05 | 2 | r15, r18 |
| 19:00:10 | 4 | r15, r18, r21, r25 |
| 19:00:15 | 2 | r21, r25 |

Request `r21` appears in the reports ending at **19:00:10** and **19:00:15**. Adding the three counts gives eight appearances across overlapping reports. The recording contains four distinct failed requests. Use the tumbling-window totals for the incident update.

</details>

Return to [the live replay](README.md#4-replay-the-sale-live) and save Query 3 before starting the job.
