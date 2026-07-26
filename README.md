# Backend Interview Scenarios

A collection of real-world backend interview scenarios designed to build production thinking rather than memorizing framework concepts.

---

# Scenario 1: Production API Suddenly Becomes Slow

## Interview Question

> It is **10:00 AM**. Your Spring Boot service normally responds in **200 ms**, but today users report that requests are taking **8–10 seconds**.
>
> You are the engineer on call.
>
> **How would you investigate and resolve this issue?**

---

# Answer

The goal is **not to immediately find the root cause**, but to **restore service quickly while performing a structured investigation**.

A good production engineer avoids guessing and follows a systematic debugging process.

---

## Step 1: Assess the Impact

Before looking at logs or changing anything, understand the scope of the problem.

Questions to ask:

- Is every endpoint slow or only one?
- Are all users affected?
- When did the issue start?
- Was there a recent deployment?
- Did any infrastructure or configuration change occur?

This helps determine whether the issue is isolated or system-wide.

---

## Step 2: Check Monitoring Dashboards

Open your monitoring platform (Grafana, Datadog, Prometheus, CloudWatch, etc.) and review:

- Response time
- Request rate
- Error rate
- CPU usage
- Memory usage
- Garbage Collection activity
- Thread pool utilization
- Database connection pool usage

These metrics usually indicate whether the application is overloaded or waiting on another dependency.

---

## Step 3: Identify the Bottleneck

Determine where time is actually being spent.

Possible areas include:

- High CPU utilization
- Database queries
- External REST APIs
- Kafka or messaging systems
- Thread contention
- Connection pool exhaustion

Avoid assumptions and let the collected data guide the investigation.

---

## Step 4: Analyze Logs

Review application logs around the time the slowdown began.

Look for:

- Long-running SQL queries
- Timeout exceptions
- Retry storms
- Circuit breaker events
- Thread pool exhaustion
- Unexpected exceptions

If correlation IDs or trace IDs are available, follow a slow request across all involved services.

---

## Step 5: Verify External Dependencies

Often the application itself is healthy, but a dependency is slow.

Check:

- Database latency
- Redis latency
- Downstream REST services
- Kafka or message queues
- Network connectivity

---

## Step 6: Mitigate the Incident

If customers are significantly impacted, consider temporary mitigations while continuing the investigation.

Possible actions:

- Roll back the latest deployment
- Scale additional application instances
- Disable non-critical features
- Apply rate limiting
- Adjust timeouts only after understanding the impact

The priority is restoring service while avoiding unnecessary risk.

---

## Step 7: Determine the Root Cause

Only after collecting sufficient evidence should the root cause be identified.

Example findings:

> The database connection pool became exhausted because a recently introduced query held connections longer than expected.

or

> A downstream payment service experienced increased latency, causing request threads to block while waiting for responses.

---

## Step 8: Prevent Future Incidents

After resolving the issue, improve the system to reduce the likelihood of recurrence.

Examples:

- Add monitoring and alerts
- Optimize slow database queries
- Configure sensible timeouts
- Introduce circuit breakers where appropriate
- Improve dashboards
- Conduct a post-incident review and document the findings

---

# What Interviewers Are Evaluating

This question is not about finding the correct technical cause.

Interviewers are evaluating whether you:

- Stay calm during production incidents
- Follow a structured debugging process
- Use metrics before making assumptions
- Prioritize customer impact
- Think about long-term prevention after resolving the issue

---

# Key Takeaways

- Do **not** jump directly to conclusions.
- Start by understanding the scope of the issue.
- Use monitoring before logs.
- Identify the actual bottleneck using evidence.
- Mitigate customer impact while investigating.
- Verify dependencies before blaming your own application.
- Fix the root cause.
- Implement preventive measures to avoid similar incidents in the future.

---

# Follow-up Challenge

Suppose **CPU usage, memory usage, and database metrics all appear normal**, yet the API still consistently takes **10 seconds** to respond.

**How would you continue investigating?**

> Think through your approach before reading the next scenario.