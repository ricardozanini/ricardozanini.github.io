---
title: "What are workflows and why do I need them?"
draft: false
date: 2026-05-15
tags: ["workflows", "java", "quarkus flow"]
categories: ["Engineering", "Workflow Patterns"]
---

In distributed architectures, managing the state of a paused or long-running process is the fastest way to entangle your core business logic with infrastructure. Consider a seemingly straightforward process: an employee requests access to a secure cloud environment. 

The sequence is simple: (a) log the request, (b) wait for a manager to approve it, and then (c) provision the access.

The inherent challenge is time. The manager might approve it in five seconds, or they might be on vacation for three days. You obviously aren't going to use `Thread.sleep()` and hold a server thread hostage waiting for a response.

To solve this, developers typically cobble together one of three workarounds:

1. **The Callback Webhook**: You build a dedicated REST endpoint just to receive the manager's approval, forcing you to generate and store correlation tokens in a database to match the incoming webhook back to the original request.

2. **The Event-Driven Choreography**: You introduce a message broker (like Kafka or RabbitMQ), write event listener services, and suddenly find yourself managing dead-letter queues and idempotency tokens just to handle a simple process pause.

3. **The Database Poller**: You build a custom state machine in the database, accompanied by a scheduled job that constantly loops over pending requests.

The polling approach is often the path of least resistance. Our initial inclination looks something like this:

```java
// The Accidental Workflow Engine
@ApplicationScoped
public class AccessProvisioningService {

    @Inject
    DatabaseRepository db;
    @Inject
    CloudProvisioner provisioner;

    // We must run this constantly to check for state changes
    @Scheduled(every = "5m")
    public void pollForApprovedRequests() {
        // Fetch all requests where status == 'APPROVED_WAITING_PROVISION'
        var pendingRequests = db.findPendingApprovals();
        
        for (var request : pendingRequests) {
            try {
                // Execute the final step
                provisioner.grantAccess(request.getUserId());
                
                // Manually update the state
                request.setStatus("COMPLETED");
                db.save(request);
            } catch (Exception e) {
                request.setStatus("FAILED");
                db.save(request);
            }
        }
    }
}
```

If we look closely at this `pollForApprovedRequests` method, we haven't just written integration code; we have inadvertently hard-coded a brittle, inefficient workflow engine. We are manually managing state transitions, burning CPU cycles on empty database queries, and tangling our infrastructure routing (the cron job) directly with our business logic (provisioning access).

To keep the system maintainable, we must separate the *process* from the *application code*. 

The Workflow Management Coalition (WfMC) formally defines a workflow as "The automation of a business process, in whole or part, during which documents, information or tasks are passed from one participant to another for action, according to a set of procedural rules" (van der Aalst et al., 2003). In pragmatic terms, a workflow engine acts as a dedicated control plane. It takes over the responsibility of routing data, tracking state, and enforcing the rules, allowing your microservices to remain stateless and focused solely on execution.

**[Quarkus Flow](https://github.com/quarkiverse/quarkus-flow)** brings this specification directly to the Java ecosystem. It is a workflow runtime library built on top of **[Quarkus](https://quarkus.io)**, allowing you to embed a lightweight workflow engine inside any microservice. Because it leverages Quarkus's build-time augmentation and compiles natively via **[GraalVM](https://www.graalvm.org)**, the engine boots in milliseconds and easily supports scale-to-zero architectures. More importantly, it allows developers to move away from opaque YAML configurations and define orchestrations using a type-safe, fluent Java DSL.

### Declarative Orchestration with Quarkus Flow

By adopting Quarkus Flow, we can refactor our polling nightmare into a declarative, event-driven state machine. Instead of manually querying databases to see if a manager has approved a request, we let the embedded engine listen for the event and route the execution natively. 

Consider the same provisioning process, now defined using the Quarkus Flow `FuncDSL`:

```java
import io.quarkiverse.flow.Flow;
import io.serverlessworkflow.api.types.FlowDirectiveEnum;
import io.serverlessworkflow.api.types.Workflow;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import static io.serverlessworkflow.fluent.func.FuncWorkflowBuilder.workflow;
import static io.serverlessworkflow.fluent.func.dsl.FuncDSL.*;

@ApplicationScoped
public class AccessProvisioningWorkflow extends Flow {

    @Inject
    CloudProvisioner provisioner;

    @Override
    public Workflow descriptor() {
        return workflow("access-provisioning")
                // 1. Awaken instantly when the Manager Approval event arrives
                .schedule(on(one("org.acme.access.approved")))
                .tasks(
                        // 2. Execute the provisioning task
                        function("provisionAccess", (AccessRequest request) -> {
                            ProvisionResult result = provisioner.grantAccess(request.getUserId());
                            return result;
                        }),

                        // 3. Evaluate the result and route the flow
                        switchWhenOrElse("checkProvisioning",
                                (ProvisionResult result) -> result.isSuccessful(),
                                "notifySuccess",
                                "notifyFailure"),

                        // 4. Branch: Success
                        consume("notifySuccess", (ProvisionResult result) -> {
                            System.out.println("Access granted for user: " + result.getUserId());
                        }).then(FlowDirectiveEnum.END),

                        // 5. Branch: Failure
                        consume("notifyFailure", (ProvisionResult result) -> {
                            System.err.println("Provisioning failed: " + result.getError());
                        }).then(FlowDirectiveEnum.END)
                )
                .build();
    }
}
```

In this definition, we model the process exactly how the [Serverless Workflow specification](https://github.com/serverlessworkflow/specification/blob/main/dsl-reference.md) intends: as a discrete list of `.tasks()`. 

The polling loop is completely eliminated. The engine natively handles the event consumption via `.schedule()`, meaning no threads are blocked while waiting for the manager's approval. The control flow is managed through explicit, readable architectural transitions (`switchWhenOrElse` and `.then()`). The engine guarantees the execution of these steps, securely tracking the state without requiring you to manage database status columns manually.

The true value of a modern workflow engine isn't just about drawing process diagrams. It is about recognizing that state management, event correlation, and process routing are infrastructure concerns, not business logic.

Every time you write a database polling job, a complex webhook correlator, or a nested try-catch compensation block, you are building an accidental, half-baked state machine. By adopting a tool like Quarkus Flow, you stop reinventing this wheel. You externalize the orchestration complexity to a dedicated control plane, leaving your microservices to do exactly what they were designed to do: execute stateless functions.
