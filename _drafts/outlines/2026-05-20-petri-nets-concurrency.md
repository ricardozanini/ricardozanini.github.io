# The hidden state machine and parallel executions

1. Introduce the problem of maintaining a parallel Java service that needs coordination of multiple API calls
2. The concept of petri nets. What they are, briefly, and how they mathematically solve the problem with AND Split pattern.
   * Duplicating the "token" (a.k.a the payload) in different `branches`.
3. How Quarkus Flow is a petri net/workflow net (if we mention workflow net here it must be estabilished when talking about Petri nets)
4. How Quarkus Flow solves this problem, brifly showing a Flow implementation. We won't dive into much details here since we will have a dedicated chapter for workflow patterns/cloud patterns problem solving blueprint
