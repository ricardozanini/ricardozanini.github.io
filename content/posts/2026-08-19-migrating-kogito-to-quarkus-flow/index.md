---
title: "From Apache KIE SonataFlow and Kogito to Quarkus Flow: Migrating to Open Workflow 1.0"
draft: false
date: 2026-08-19
tags: ["workflows", "java", "quarkus flow", "kogito", "migration", "open workflow specification"]
categories: ["Engineering", "Workflow Patterns"]
---

If you have been running Kogito Serverless Workflows in production, you are likely aware that the landscape has shifted. The specification evolved from version 0.8 to 1.0.0, the project was [renamed from Serverless Workflow to Open Workflow Specification](https://openworkflow.cloud/blog/general/announcing-rename-to-open-workflow-specification/), and the runtime engine moved from Apache KIE to the Quarkiverse. This is not a minor version bump — it is a fundamental paradigm shift in how workflows are defined, moving from explicit state declarations to task-based orchestration.

The good news: **[Quarkus Flow](https://github.com/quarkiverse/quarkus-flow)** is the natural migration path. It implements the [Open Workflow Specification](https://github.com/open-workflow-specification/specification) 1.0.0 on a completely rewritten engine, purpose-built for the new spec's semantics. This post explains why we sunset Apache KIE SonataFlow and created Quarkus Flow, what changed in the specification, and how to migrate your existing workflows.

### Why the Sunset Happened

Apache KIE — the organization behind Kogito — maintains a portfolio of business automation technologies: DMN for decision modeling, Drools for rule engines, and jBPM for BPMN process execution. When the Serverless Workflow specification emerged, Kogito implemented it by building an adapter layer on top of jBPM.

This architectural choice created friction. jBPM is a BPMN engine designed for long-running business processes with human tasks, compensation handlers, and complex transaction semantics. The Serverless Workflow specification, by contrast, describes lightweight, event-driven orchestrations optimized for cloud-native microservices. Mapping the spec's callback states, event correlations, and parallel branches onto jBPM's internal model required significant workaround code — and that complexity leaked into the runtime behavior.

The [Open Workflow Specification](https://github.com/open-workflow-specification/specification) (OWS) 1.0.0 took a different direction. The DSL moved away from explicit state declarations in favor of a task-based model with implicit flow control. None of the KIE or Kogito API contracts make sense for this new architecture. Rather than force-fitting the new spec onto jBPM, we understood it was time to build a clean-room implementation backed by the official OWS Java SDK, while allowing jBPM and Kogito to do what they do best — Business Automation.

The result is Quarkus Flow: a lightweight, CDI-first workflow engine with zero jBPM inheritance.

### The Specification Change: States to Tasks

The most significant change between 0.8 and 1.0.0 is how workflows are defined. Version 0.8 required explicit state declarations with eight state types (`Event`, `Operation`, `Switch`, `Inject`, `ForEach`, `Parallel`, `Sleep`, `Callback`) connected by `transition` properties. Version 1.0.0 replaces this DSL with a task-based model where execution flows implicitly through an ordered list of tasks.

| Aspect | 0.8.x (States) | 1.0.0 (Tasks) |
|--------|----------------|---------------|
| Execution unit | 8 state types | 13+ task types |
| Flow control | Explicit `transition` to next state | Implicit sequential flow + `then` directive |
| Document structure | Flat (`id`, `name`, `version` at root) | Nested under `document:` with `namespace` |
| Reusable components | `functions:`, `events:`, `retries:` at root | Consolidated under `use:` block |
| Data transformation | `stateDataFilter`, `actionDataFilter` | Unified `input`/`output` on tasks |
| Expression language | JsonPath or jq | jq as default |

The mapping from old state types to new task types is mostly straightforward:

| 0.8.x State | 1.0.0 Task | Notes |
|-------------|------------|-------|
| Callback State | `listen` task | Wait for CloudEvent with correlation |
| Operation State | `call` task | HTTP, OpenAPI, gRPC, or custom function |
| Switch State | `switch` task | Data conditions with jq expressions |
| Inject State | `set` task | Direct data injection |
| ForEach State | `for` task | Iteration over collections |
| Parallel State | `fork` task | Concurrent branch execution |
| Sleep State | `wait` task | Duration or timestamp |
| Event State | `listen` task | Consume one or more events |

Version 1.0.0 also introduces capabilities that had no equivalent in 0.8:

- **`try` task**: First-class error handling with catch blocks, replacing the `onErrors` state property
- **`emit` task**: Dedicated CloudEvent emission as a discrete step
- **`run` task**: Execute containers, shell commands, scripts, or subworkflows
- **A2A Call**: Agent-to-Agent protocol for AI agent communication
- **MCP Call**: Model Context Protocol for AI tooling integration

### Quarkus Flow: A Complete Rewrite

Quarkus Flow is not "Kogito without SonataFlow." It is a fundamentally different engine built from scratch on the official OWS Java SDK. The architecture is 100% Quarkus-native: CDI-first dependency injection, build-time augmentation for fast startup, and seamless native compilation via GraalVM.

Because Quarkus Flow is a standard Quarkus extension, it integrates naturally with the rest of the ecosystem. Add [`quarkus-micrometer-registry-prometheus`](https://quarkus.io/guides/micrometer) to your classpath and you get workflow metrics. Add [`quarkus-opentelemetry`](https://quarkus.io/guides/opentelemetry) and you get distributed tracing. No special configuration required — the extension participates in CDI like any other bean.

There are three ways to define workflows:

**1. Java DSL** — Type-safe fluent API for developers who want compile-time validation and IDE support:

```java
@ApplicationScoped
public class OrderWorkflow extends Flow {
    @Inject
    OrderService orderService;

    @Override
    public Workflow descriptor() {
        return FlowWorkflowBuilder.workflow("order-processing")
            .tasks(
                call("validateOrder", (Order o) -> orderService.validate(o)),
                fork("parallelFulfillment",
                    call("reserveInventory", (Order o) -> orderService.reserve(o)),
                    call("calculateShipping", (Order o) -> orderService.ship(o))),
                set("complete", "{ status: \"fulfilled\" }"))
            .build();
    }
}
```

**2. YAML/JSON files** — Zero Java code, ideal for non-Java developers or GitOps workflows:

```yaml
document:
  dsl: '1.0.0'
  namespace: acme
  name: order-processing
  version: '1.0.0'
do:
  - validateOrder:
      call: http
      with:
        method: POST
        endpoint: ${.validationService}/validate
  - parallelFulfillment:
      fork:
        branches:
          - reserveInventory:
              call: http
              with:
                method: POST
                endpoint: ${.inventoryService}/reserve
          - calculateShipping:
              call: http
              with:
                method: POST
                endpoint: ${.shippingService}/calculate
  - complete:
      set:
        status: fulfilled
```

**3. Hybrid** — Combine YAML workflow definitions with Java-implemented functions via CDI to get the best of both worlds: declarative orchestration with type-safe business logic.

For teams without Java expertise, the [**Runner** extension](https://docs.quarkiverse.io/quarkus-flow/dev/runner.html) provides a turnkey deployment model. Add your YAML or JSON workflow files to the classpath or a system directory, and Quarkus Flow automatically generates REST endpoints to start and manage workflow instances. No application code required. You can try it immediately with the pre-built container image:

```bash
docker run --rm -p 8080:8080 \
  -v ~/my-workflows:/deployments/workflows:ro \
  quay.io/quarkiverse/quarkus-flow-runner:latest-minimal
```

### The Cloud Platform Story

Kogito offered a complete cloud-native stack: the SonataFlow Operator for Kubernetes deployment, Data Index for querying workflow state, and Jobs Service for timer scheduling. Where does Quarkus Flow stand?

**Available today:**
- **Embedded library**: Full control over deployment architecture — container, serverless, or traditional application server
- **Persistence**: MVStore (embedded), JPA (PostgreSQL, MySQL, etc.), or Redis backends
- **Messaging**: SmallRye Reactive Messaging integration with tested support for Kafka, AMQP, and RabbitMQ
- **Scheduling**: Quartz integration via [`quarkus-quartz`](https://quarkus.io/guides/quartz) replaces the standalone Jobs Service — timers and delayed tasks run within your application using Quarkus's native scheduler infrastructure
- **Observability**: Out-of-the-box integration with [Micrometer](https://quarkus.io/guides/micrometer) and [OpenTelemetry](https://quarkus.io/guides/opentelemetry)

**Under development at [kubesmarts.org](https://kubesmarts.org):**
- **Logic Operator**: Kubernetes operator for managing workflow deployments ([kubesmarts/logic-apps](https://github.com/kubesmarts/logic-apps/))
- **Data Index**: Read-only GraphQL query service for workflow execution data, supporting FluentBit or Kafka event ingestion with PostgreSQL or Elasticsearch storage ([documentation](https://kubesmarts.org/logic-apps/data-index/1.0.0/index.html))

The cloud platform components are being built in parallel with the core engine. Users who need Kubernetes-native deployment today can use Quarkus Flow as an embedded library within standard Quarkus applications deployed via Helm, Kustomize, or any container orchestration tool.

### AI Integration: A2A and MCP

Quarkus Flow is positioned for the agentic AI future. Today, the [`quarkus-flow-langchain4j`](https://docs.quarkiverse.io/quarkus-flow/dev/langchain4j.html) module provides deep integration with LangChain4j, including declarative agentic workflow annotations (`@SequenceAgent`, `@ParallelAgent`, `@LoopAgent`) and human-in-the-loop patterns.

```xml
<!-- Add to pom.xml for LangChain4j integration -->
<dependency>
    <groupId>io.quarkiverse.flow</groupId>
    <artifactId>quarkus-flow-langchain4j</artifactId>
</dependency>

<!-- Plus your preferred LLM provider -->
<dependency>
    <groupId>io.quarkiverse.langchain4j</groupId>
    <artifactId>quarkus-langchain4j-ollama</artifactId>
</dependency>
```

The roadmap includes:
- **A2A tasks**: The Agent-to-Agent protocol is already implemented in the OWS Java SDK and will transition to Quarkus Flow shortly
- **MCP tasks**: Model Context Protocol calls for AI tool integration
- **Quarkus Flow as MCP server**: Expose workflows as tools that AI systems like Claude or LangChain4j agents can discover and execute

### Managing the Cutover: Zero-Downtime Deployment

Because engine serialization formats are fundamentally different, active data migration from Kogito to Quarkus Flow is not possible. However, this does not mean you need to schedule a maintenance window or endure downtime. Instead, you can rely on standard routing patterns to achieve a smooth transition.

We recommend a side-by-side deployment approach (often referred to as the Strangler Fig pattern) allowing workflows to drain naturally:

1. **Deploy Side-by-Side**: Deploy your new 1.0.0 Quarkus Flow applications alongside your legacy 0.8.x Kogito deployments. Both engines will run concurrently during the migration window.

2. **Route New Traffic**: Configure your API Gateway, Ingress controller, or service mesh to route all new workflow initiation requests to the Quarkus Flow endpoints.

3. **Drain Legacy Instances**: Route any continuation requests, callbacks, or task completions tied to existing legacy workflow IDs to the Kogito deployment. Kogito will continue processing these in-flight instances until they reach their terminal state.

4. **Decommission**: Monitor your metrics (via [Micrometer](https://quarkus.io/guides/micrometer)) or your Data Index. Once the active instance count on the Kogito deployment drops to zero, you can safely spin down the legacy infrastructure.

By relying on ingress routing and natural draining rather than a hard cutover or complex database migration scripts, you can execute a risk-free migration while ensuring a seamless experience for upstream consumers.

### Migration: What You Can Automate

**Messaging migration is straightforward.** Both specification versions mandate CloudEvents for inter-service communication, and that contract has not changed. Your channel configuration (`flow-in`, `flow-out`) and broker setup (Kafka, AMQP, RabbitMQ) will work with minimal adjustments.

**A CLI migration tool is under development.** The OWS community is building a multi-platform tool to automate the mechanical transformation of 0.8/0.9 workflow definitions to 1.0.0 format. The tool targets 80%+ automatic migration coverage, with detailed reports identifying patterns that require manual review. The [ADR](https://github.com/open-workflow-specification/specification/blob/main/adr/v1.0-adr-migration-tool.md) documents the design and scope.

### Migration Walkthrough: Callback Pattern

The callback pattern — call an external service, wait for an asynchronous response event, continue processing — is one of the most common orchestration primitives. Here is how it transforms from 0.8 to 1.0.0.

**Before (Kogito 0.8.x):**

```json
{
  "id": "callback_workflow",
  "version": "1.0",
  "specVersion": "0.8",
  "name": "Callback Workflow",
  "start": "CallExternalService",
  "events": [
    {
      "name": "callbackEvent",
      "source": "external-service",
      "type": "callback.received.v1"
    }
  ],
  "functions": [
    {
      "name": "callbackFunction",
      "type": "rest",
      "operation": "classpath:specs/external-service.yaml#sendRequest"
    }
  ],
  "states": [
    {
      "name": "CallExternalService",
      "type": "callback",
      "action": {
        "functionRef": {
          "refName": "callbackFunction",
          "arguments": {
            "requestId": "$.requestId"
          }
        }
      },
      "eventRef": "callbackEvent",
      "transition": "ProcessResult"
    },
    {
      "name": "ProcessResult",
      "type": "inject",
      "data": {
        "status": "completed"
      },
      "end": true
    }
  ]
}
```

**After (Quarkus Flow 1.0.0 — YAML):**

```yaml
document:
  dsl: '1.0.0'
  namespace: acme
  name: callback-workflow
  version: '1.0.0'
do:
  - callExternalService:
      call: openapi
      with:
        document:
          endpoint: specs/external-service.yaml
        operationId: sendRequest
        arguments:
          requestId: ${.requestId}
  - waitForCallback:
      listen:
        to:
          one:
            with:
              type: callback.received.v1
  - processResult:
      set:
        status: completed
```

**After (Quarkus Flow 1.0.0 — Java DSL):**

```java
@ApplicationScoped
public class CallbackWorkflow extends Flow {

    @Override
    public Workflow descriptor() {
        return FlowWorkflowBuilder.workflow("callback-workflow", "acme", "1.0.0")
            .tasks(
                call("callExternalService",
                    openapi("specs/external-service.yaml", "sendRequest")),
                listen("waitForCallback", toOne("callback.received.v1")),
                set("processResult", "{ status: \"completed\" }"))
            .build();
    }
}
```

The transformation is mechanical:
1. The `events` and `functions` declarations at the root level disappear — they are inlined or referenced directly in tasks
2. The `callback` state becomes two discrete tasks: a `call` to invoke the external service and a `listen` to wait for the response event
3. The `inject` state becomes a `set` task
4. Explicit `transition` properties are eliminated — tasks execute sequentially in the `do` array

### Key Migration Patterns

| Kogito 0.8.x Pattern | Quarkus Flow 1.0.0 Equivalent |
|----------------------|-------------------------------|
| `type: callback` + `eventRef` | `call` task followed by `listen` task with `toOne()` |
| `type: operation` + `actions[]` | Sequential `call` tasks |
| `type: switch` + `dataConditions` | `switch` task with jq condition expressions |
| `type: parallel` + `branches` | `fork` task with inline branch definitions |
| `type: foreach` + `inputCollection` | `for` task iterating over a jq expression |
| `functionRef` to OpenAPI spec | `call: openapi` with document and operationId |
| `stateDataFilter.output` | `output:` property on the task |
| `onErrors` array | `try` task with `catch` blocks |

### Workflow Composition

Version 1.0.0 introduces first-class support for workflow composition via the `subflow` task. Parent workflows can invoke child workflows by namespace, name, and version:

```java
@ApplicationScoped
public class ParentWorkflow extends Flow {
    @Override
    public Workflow descriptor() {
        return FlowWorkflowBuilder.workflow("parent-workflow", "org.acme", "1.0")
            .tasks(
                subflow("executeValidation",
                    workflow("org.acme", "validation-workflow", "1.0")),
                subflow("executeProvisioning",
                    workflow("org.acme", "provisioning-workflow", "1.0")))
            .build();
    }
}
```

This enables modular workflow design where common orchestration patterns can be extracted into reusable components.

### Testing and Observability

**Testing** relies entirely on the Quarkus Test framework. Workflow integration tests use `@QuarkusTest` annotations, and you can inject workflow instances directly into test methods. The team is also developing additional testing utilities to facilitate debugging and step-through interaction with the OWS flow engine.

**Observability** is built in. Quarkus Flow emits workflow and task metrics via Micrometer and participates in distributed traces via OpenTelemetry. Because the extension is a standard CDI participant, it automatically integrates with whatever observability stack you configure for your Quarkus application:

```xml
<!-- Add to pom.xml for Prometheus metrics -->
<dependency>
    <groupId>io.quarkus</groupId>
    <artifactId>quarkus-micrometer-registry-prometheus</artifactId>
</dependency>

<!-- Add for OpenTelemetry tracing -->
<dependency>
    <groupId>io.quarkus</groupId>
    <artifactId>quarkus-opentelemetry</artifactId>
</dependency>
```

No additional configuration is required. Workflow starts, task executions, and completion events appear in your metrics dashboards and trace visualizations automatically.

### What Comes Next

Quarkus Flow is the natural migration path for Kogito Serverless Workflow users. The specification change from 0.8 to 1.0.0 is significant, but the concepts map cleanly: states become tasks, transitions become implicit flow, and the overall structure is simpler and more declarative.

Start planning your migration now:
1. Inventory your existing workflows and identify the state types in use
2. Let running instances complete on Kogito before migrating definitions
3. Use the mapping tables above to translate state-based workflows to task-based equivalents
4. For YAML/JSON workflows, the Runner extension provides the same zero-code deployment model you may be familiar with from Kogito

The cloud platform components — operator, Data Index, and Jobs Service equivalents — are being built in parallel. The AI integrations position Quarkus Flow for the agentic future where workflows orchestrate not just microservices, but AI agents.

**Resources:**
- [Quarkus Flow GitHub](https://github.com/quarkiverse/quarkus-flow)
- [Open Workflow Specification](https://github.com/open-workflow-specification/specification)
- [OWS 1.0.0 DSL Reference](https://github.com/open-workflow-specification/specification/blob/main/dsl-reference.md)
- [Migration Tool ADR](https://github.com/open-workflow-specification/specification/blob/main/adr/v1.0-adr-migration-tool.md)
- [Data Index Documentation](https://kubesmarts.org/logic-apps/data-index/1.0.0/index.html)
- [Why "Open Workflow" not "Serverless Workflow"](https://openworkflow.cloud/blog/general/announcing-rename-to-open-workflow-specification/)
