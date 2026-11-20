# SAQL reference · How to read a ticket-rush query

In Azure Stream Analytics Query Language (SAQL), each event in the ticket stream supplies one request’s fields. Use this guide to work out which events a query reads and what one row of its output means. The examples use the recording from the [student walkthrough](README.md#3-investigate-the-recording).

## Read a query

**When writing a query**, start with `SELECT` (result fields), `INTO` (the lab output), and `FROM` (the lab input). Add `TIMESTAMP BY` after the input name to choose the clock. The remaining clauses are optional and appear in this order. `WHERE` filters requests, `GROUP BY` collects them into groups, and `HAVING` filters those groups. [Microsoft’s SELECT reference](https://learn.microsoft.com/en-us/stream-analytics-query/select-azure-stream-analytics)

For example, **which device types made at least two requests to the seats page in a single ten-second interval?**

```sql
SELECT System.Timestamp() AS windowEnd, deviceType, COUNT(*) AS seatRequests
INTO [ticket-results]
FROM [ticket-requests] TIMESTAMP BY eventTime
WHERE path = '/seats'
GROUP BY deviceType, TumblingWindow(second, 10)
HAVING COUNT(*) >= 2
```

**To read it, follow a request starting at `FROM`.** You can use the same questions for other queries and skip the clauses they omit.

| Ask yourself | Read this part of the example |
| --- | --- |
| Where do the events come from, and which clock do I use? | `FROM [ticket-requests]` names the input connection. `TIMESTAMP BY eventTime` uses the recorded completion time. The default clock for an Event Hub input is the time the hub received the event. |
| Does this request qualify? | `WHERE path = '/seats'` compares the request’s `path` field with the text value `'/seats'`. Only matching requests reach the count. |
| Which requests belong together? | `GROUP BY deviceType, TumblingWindow(second, 10)` puts requests from the same device type and fixed ten-second interval together. |
| Is the group large enough to report? | `COUNT(*)` counts the requests in one group. `HAVING COUNT(*) >= 2` keeps groups with at least two. |
| What does one result row tell me? | `SELECT` returns the window’s end time, the device type, and the count. `AS` gives a result field a name, such as `seatRequests`. |
| Where does the row go? | `INTO [ticket-results]` names the output connection. Square brackets enclose the lab’s hyphenated connection names. |

**Trace one result.** In the interval ending at 19:00:10 UTC, `r16` and `r22` both requested `/seats` on a phone. Together they produce a row with `deviceType = phone` and `seatRequests = 2`. The desktop request `r17` also passes `WHERE`. It is the only desktop seats request in that interval, so its group drops out at `HAVING`.

**What if you need two counts?** In Query 3, a success and a failure both contribute to total traffic. `COUNT(*)` counts them both, while `SUM(CASE WHEN statusCode >= 500 THEN 1 ELSE 0 END)` adds one only for each server error. Adding `WHERE statusCode >= 500` would remove the successes before either calculation. [Return to Query 3](README.md#query-3-how-did-traffic-change-during-the-rush)

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

**Read the challenge query**

1. `TIMESTAMP BY eventTime` uses the recorded clock. `WHERE` admits a request only if it reached `/checkout` and its status was 500 or higher.
2. Read `HoppingWindow(second, 10, 5)` as “look back **10 seconds**, move forward **5 seconds**, and repeat.” `GROUP BY deviceType` keeps separate counts for phones and desktops within each window.
3. Each result row tells you how many failed checkouts (`failedCheckouts`) one device type had in the ten seconds ending at `windowEnd`. To make your prediction for `r21`, find every window that contains 19:00:07 UTC.

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
