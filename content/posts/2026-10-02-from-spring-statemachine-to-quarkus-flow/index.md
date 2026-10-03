---
title: "From Spring StateMachine to Quarkus Flow: A Maintained Open-Source Path for State-Driven Apps"
draft: false
date: 2026-10-02
tags: ["workflows", "java", "quarkus flow", "spring statemachine", "state machine", "migration", "open workflow specification"]
categories: ["Engineering", "Workflow Patterns"]
---

If you built your order lifecycles, approval flows, or device state tracking on **Spring StateMachine**, the ground has shifted under you. On [April 21, 2025](https://spring.io/blog/2025/04/21/spring-cloud-data-flow-commercial/), the Spring team announced that Spring StateMachine — alongside Spring Cloud Data Flow and Spring Cloud Deployer — would no longer be maintained as an open-source project. The `4.0.x` line is the last open-source release; future work lands only in the commercial Tanzu Spring subscription.

The existing code is not going anywhere, and if you have a paid subscription this may be a non-event. But if staying on a **maintained, fully open-source** foundation matters to you — and if your state machines have quietly grown into something that waits on events, survives restarts, and spans services — this is a good moment to reassess.

There's also a structural reason to care about *what* you build on. Quarkus Flow is an implementation of the [Open Workflow Specification](https://github.com/open-workflow-specification/specification), a vendor-neutral **CNCF-sandbox standard** (the successor to Serverless Workflow). You define workflows against an open specification, not a single company's product — so no vendor can relicense the format out from under you. That is precisely the kind of lock-in a move away from a now-commercial runtime is meant to avoid.

This post makes the case for **[Quarkus Flow](https://github.com/quarkiverse/quarkus-flow)** as that alternative. It is also a state machine — a workflow is a state machine — but it models one differently, and it is not a drop-in replacement JAR. What follows is a concept-by-concept mapping, a concrete before/after of the *same* order lifecycle on both frameworks, a practical "is this right for you?" section, and answers to the questions a Spring StateMachine user will actually ask.

### Both Are State Machines

**A workflow *is* a state machine.** Quarkus Flow is a state-machine engine, so this isn't a move from a state machine to something foreign — it's a move between two ways of *modeling* the same idea.

The first difference is notation. Spring StateMachine is **declarative about states**: you enumerate states and the transitions between them, hold a reference to the machine, send it events synchronously, and ask `machine.getState()` at any time. Quarkus Flow is **declarative about tasks**: you write an ordered list of tasks (the Open Workflow Specification 1.0.0 DSL — and, under the hood, Quarkus Flow runs on the specification's official Open Workflow Java SDK), and "state" is *positional* — it's wherever the instance is currently parked. Same machine, different notation.

The second difference is reach, and it is **opt-in**. By default, a Quarkus Flow instance runs in the JVM and is discarded once it completes — exactly Spring StateMachine's memory model. The execution happens and dies in-process; nothing is written anywhere. When you need more, you turn it on: add a persistence backend and a parked instance survives restarts; add messaging and it spans services. You don't pay for durability or distribution unless you ask for them.

| Dimension | Spring StateMachine | Quarkus Flow |
|-----------|---------------------|--------------|
| Model | State machine, declared as explicit states + transitions | State machine, modeled as ordered tasks |
| "State" | Enumerated, `getState()` | Positional — the current task + lifecycle status |
| Execution | Synchronous, in-process | In-process by default; event-driven once you use `listen`/`wait` |
| Waiting | In memory | In memory by default; durable *when persistence is enabled* |
| Scope | One JVM | One JVM by default; distributed *when messaging is enabled* |
| Events | `sendEvent(...)` | CloudEvents, correlated to the instance |
| Runtime | A library you embed | A Quarkus extension (CDI-first, native-ready) |
| Persistence | `StateMachinePersist` (you wire it) | Optional, built-in: Redis / JPA / MVStore |
| License | OSS frozen at 4.0.x; future is commercial | Apache 2.0, actively developed |

Neither notation is "better." Spring StateMachine is a focused, in-process FSM. Quarkus Flow is a state-machine engine that *starts* in the same place — in-process and ephemeral — and gives you a growth path to durable, event-driven, distributed orchestration when, and only when, you need it.

### Is Quarkus Flow Right for You?

Both are state machines, so the question isn't "state machine or not" — it's which tool fits the shape of your problem.

**Quarkus Flow is a great fit when your machine:**

- Waits on **external events** (a payment callback, a human approval, a device ping) rather than purely in-memory signals.
- Needs to **survive restarts** — an order mid-flight should still be mid-flight after a deploy.
- Spans **multiple services** or needs to emit/consume CloudEvents.
- Calls out to **HTTP, gRPC, OpenAPI, or AI agents** between transitions.
- Benefits from **built-in persistence, metrics, tracing, retries, and suspend/resume** you'd otherwise hand-roll.
- Is heading toward **agentic/AI orchestration** (Quarkus Flow has first-class LangChain4j integration).

**Flow may be more engine than you need when:**

- You need **ultra-low-latency transitions on a hot path** — every transition completing in microseconds with no I/O. This is the one case where a hand-tuned embedded FSM genuinely wins; an orchestration engine adds overhead a tight in-process loop avoids.
- Your state lives **entirely inside one frontend or one request** — a UI wizard, a toggle, a form step — and never needs persistence, events, or external calls. Flow runs that in-process just fine, but Spring StateMachine `4.0.x` already does the job; if the frozen OSS line is acceptable to you, there's no urgency to move.

**Embedded, not a separate cluster.** If you've weighed Temporal, Camunda/Zeebe, or Conductor for durability and event-driven orchestration, note the trade-off: those run as standalone control planes you deploy, scale, and operate. Quarkus Flow gives you the same durability, retries, and suspend/resume *inside your service* through CDI — no extra cluster to run. When you want orchestration capabilities without the operational weight of a dedicated engine, that in-process model is the sweet spot.

Everything else — event-driven, long-running, distributed, or simply wanting a maintained open-source foundation — is squarely in Flow's wheelhouse.

### Concept Mapping

Here is how the Spring StateMachine vocabulary maps onto Quarkus Flow's Java DSL. Most of it maps cleanly; the few exceptions are flagged below.

| Spring StateMachine | Quarkus Flow | Notes |
|---------------------|--------------|-------|
| **State** (a node you rest in) | A task position — e.g. a `listen` or `wait` task is a "waiting state" | State is positional, not declared |
| **Event** | A **CloudEvent** consumed by a `listen` task, correlated by instance id | `sendEvent` becomes "publish a correlated CloudEvent" |
| **Transition** (external) | `.then("nextTask")` — jump to *any* named task | Uniform mechanism for every transition kind |
| **Guard** | `switchWhen(...)` / `switchWhenOrElse(...)` | A predicate that routes flow |
| **Action** (entry/exit/do/transition) | `function("name", this::method, Type.class)` — a **call task** to a plain Java method | ✅ A strength: actions become first-class, reusable, independently testable tasks |
| **Choice / Junction** pseudostate | `switchCase(...)` with `caseOf(...)` / `caseDefault(...)` | N-way branching on data |
| **Fork** pseudostate | `fork("name", branchA, branchB)` | Branches run concurrently |
| **Join** pseudostate | `fork` merges all branches by default (`compete(false)`) | `compete(true)` = first-wins instead |
| **Composite / hierarchical state** | `subflow(workflow(ns, name, version))` | A state that contains a nested machine → a reusable sub-workflow |
| **Regions** (orthogonal/parallel) | `fork` with concurrent branches | |
| **Extended state** (variables) | `set("key", value)` / `set(map)`; read via jq or `withContext((in, ctx) -> ...)` | ✅ A strength: data flows through tasks naturally |
| **Self / internal transition** (loop / back-transition) | `.then("anEarlierTask")` — arbitrary cycles | True "stay in place and run an action" is only approximated (state is positional). For *retries*, use the built-in retry policy, not a manual loop |
| **Listeners** (`StateMachineListener`) | `WorkflowExecutionListener` (`onTaskStarted`, `onTaskCompleted`, …) | ✅ Richer: workflow- *and* task-level hooks |
| **Interceptors** (`StateMachineInterceptor` — `preEvent` / `preStateChange` / `preTransition`) | An explicit `switchWhen` guard or `try`/`catch` task | Flow's listener only *observes*; to **veto or mutate** before a step, model it in the flow rather than in a decoupled, out-of-band hook |
| **`StateMachinePersist`** | *Optional* built-in persistence (Redis / JPA / MVStore) that resumes at the exact parked position | ✅ A strength: durable, zero hand-wiring — and opt-in |
| **`getState()`** | `status()` (lifecycle) + current task via listener/trace | See "What state am I in?" below |
| **Error handling / `onError`** | `try`/`catch` tasks + a retry/backoff subsystem | ✅ A strength: retries and backoff are built in |
| **History pseudostate** (resume last substate) | ⚠️ By persistence + listeners | No dedicated construct; durable persistence resumes a *live* parked instance, and a listener can record the execution path for audit (a terminated instance can't be re-run) |
| **Entry / exit points** into submachines | ⚠️ By composition | No dedicated construct; reproduce via multiple sub-workflows or a parameterized one (see note below the table) |

Two takeaways. First, the things Spring StateMachine users lean on most — states, events, guards, actions, choice, fork/join, composition, extended state, listeners, persistence — all map, and several map onto *strengths* (durability, retries, actions-as-tasks). Second, two items have no *dedicated* construct — **history** pseudostates and **named entry/exit points** — but neither is a dead end: history is covered by durable persistence (resume-where-parked, for live instances) plus listener-captured execution history when you want an audit trail, and entry/exit points are reproducible by composition (see the note below). If one of these is load-bearing in your design, confirm the modelling before you commit.

> **On entry/exit points.** Spring StateMachine's entry/exit points let a transition enter a submachine at a specific internal substate (not just its initial state), or leave through a named exit wired to a different parent target — so one reusable submachine can be entered and left in several ways. In Quarkus Flow this is a modelling choice rather than a missing feature. Either decompose into several small sub-workflows — one per entry path — and have the parent call whichever one it means; or keep a single sub-workflow and parameterize it: pass a `mode` field so its first task routes to the right starting task (entry points), and follow the `subflow(...)` call with a `switchWhenOrElse(...)` on the returned outcome to resume the parent differently (exit points). It's an advanced, rarely-used feature, so the gap is in notation, not expressiveness.

### Walkthrough: The Same Order Lifecycle, Both Ways

Let's make this concrete with the canonical order lifecycle:

```
          place order
   NEW ───────────────▶ AWAITING_PAYMENT
                               │  PAYMENT event
                               ▼
                         (guard: approved?)
                          │            │
                   approved            rejected
                          ▼            ▼
                        PAID        CANCELLED (end)
                          │ fulfill
                          ▼
                   AWAITING_SHIPMENT
                          │  SHIPMENT event
                          ▼
                      DELIVERED (end)
```

#### Before — Spring StateMachine

States and events are enums; the machine is assembled with the configurer DSL. Guards decide which transition fires; actions are callbacks bolted onto transitions.

```java
public enum OrderStates { NEW, AWAITING_PAYMENT, PAID, AWAITING_SHIPMENT, DELIVERED, CANCELLED }
public enum OrderEvents { PLACE, PAYMENT_RECEIVED, SHIPMENT_DISPATCHED }
```

```java
@Configuration
@EnableStateMachine
public class OrderStateMachineConfig
        extends StateMachineConfigurerAdapter<OrderStates, OrderEvents> {

    @Override
    public void configure(StateMachineStateConfigurer<OrderStates, OrderEvents> states)
            throws Exception {
        states.withStates()
            .initial(OrderStates.NEW)
            .state(OrderStates.AWAITING_PAYMENT)
            .state(OrderStates.PAID)
            .state(OrderStates.AWAITING_SHIPMENT)
            .end(OrderStates.DELIVERED)
            .end(OrderStates.CANCELLED);
    }

    @Override
    public void configure(StateMachineTransitionConfigurer<OrderStates, OrderEvents> transitions)
            throws Exception {
        transitions
            .withExternal()
                .source(OrderStates.NEW).target(OrderStates.AWAITING_PAYMENT)
                .event(OrderEvents.PLACE)
                .action(placeOrderAction())
            .and()
            .withExternal()                                   // guard routes here...
                .source(OrderStates.AWAITING_PAYMENT).target(OrderStates.PAID)
                .event(OrderEvents.PAYMENT_RECEIVED)
                .guard(paymentApproved())
                .action(fulfillOrderAction())
            .and()
            .withExternal()                                   // ...or here
                .source(OrderStates.AWAITING_PAYMENT).target(OrderStates.CANCELLED)
                .event(OrderEvents.PAYMENT_RECEIVED)
                .guard(paymentRejected())
                .action(cancelOrderAction())
            .and()
            .withExternal()
                .source(OrderStates.PAID).target(OrderStates.AWAITING_SHIPMENT)
            .and()
            .withExternal()
                .source(OrderStates.AWAITING_SHIPMENT).target(OrderStates.DELIVERED)
                .event(OrderEvents.SHIPMENT_DISPATCHED)
                .action(completeOrderAction());
    }

    @Bean
    public Guard<OrderStates, OrderEvents> paymentApproved() {
        return ctx -> ((PaymentEvent) ctx.getMessageHeader("payment")).approved();
    }

    @Bean
    public Guard<OrderStates, OrderEvents> paymentRejected() {
        return ctx -> !((PaymentEvent) ctx.getMessageHeader("payment")).approved();
    }

    // placeOrderAction(), fulfillOrderAction(), cancelOrderAction(),
    // completeOrderAction() are Action<OrderStates, OrderEvents> beans.
}
```

To drive it, you hold the machine and send events synchronously:

```java
machine.start();
machine.sendEvent(OrderEvents.PLACE);
machine.sendEvent(MessageBuilder.withPayload(OrderEvents.PAYMENT_RECEIVED)
        .setHeader("payment", new PaymentEvent("ORDER#1", true, "PAY-1")).build());
OrderStates current = machine.getState().getId();   // explicit state
```

#### After — Quarkus Flow

The same lifecycle becomes an ordered list of tasks. The waiting states (`AWAITING_PAYMENT`, `AWAITING_SHIPMENT`) become `listen` tasks that park the instance until a correlated CloudEvent arrives. The guard becomes a `switchWhenOrElse`. Every action becomes a `function` call task invoking a plain Java method.

```java
@ApplicationScoped
public class OrderLifecycleWorkflow extends Flow {

    static final String PAYMENT_RECEIVED   = "org.acme.order.payment.received";
    static final String SHIPMENT_DISPATCHED = "org.acme.order.shipment.dispatched";

    @Override
    public Workflow descriptor() {
        return FlowWorkflowBuilder.workflow("order-lifecycle", "examples")  // workflow(name, namespace)
            .tasks(
                // NEW: starting the instance IS the "PLACE" trigger
                function("placeOrder", this::placeOrder, OrderRequest.class),

                // AWAITING_PAYMENT: park until a correlated payment CloudEvent arrives
                listen("awaitPayment",
                    toOne(consumed(PAYMENT_RECEIVED).extensionByInstanceId("flowinstanceid"))),

                // guard: approved? -> fulfillOrder (PAID) else -> cancelOrder (CANCELLED)
                switchWhenOrElse(PaymentEvent::approved, "fulfillOrder", "cancelOrder",
                    PaymentEvent.class),

                // PAID + entry action
                function("fulfillOrder", this::fulfillOrder, PaymentEvent.class),

                // AWAITING_SHIPMENT: park until a correlated shipment CloudEvent arrives
                listen("awaitShipment",
                    toOne(consumed(SHIPMENT_DISPATCHED).extensionByInstanceId("flowinstanceid"))),

                // DELIVERED (end)
                function("completeOrder", this::completeOrder, ShipmentEvent.class)
                    .then(FlowDirectiveEnum.END),

                // CANCELLED (end)
                function("cancelOrder", this::cancelOrder, PaymentEvent.class)
                    .then(FlowDirectiveEnum.END))
            .build();
    }

    OrderRequest placeOrder(OrderRequest req) { /* [NEW]  ... */ return req; }
    OrderResult  fulfillOrder(PaymentEvent p) { /* [PAID] ... */ return new OrderResult(p.orderId(), "FULFILLING", p.reference()); }
    OrderResult  completeOrder(ShipmentEvent s) { /* [DELIVERED] ... */ return new OrderResult(s.orderId(), "DELIVERED", s.trackingId()); }
    OrderResult  cancelOrder(PaymentEvent p) { /* [CANCELLED] ... */ return new OrderResult(p.orderId(), "CANCELLED", p.reference()); }
}
```

You start an instance (that is your `PLACE`), then drive it with correlated CloudEvents instead of in-memory `sendEvent` calls:

```java
WorkflowInstance instance = workflow.instance(new OrderRequest("ORDER#1", "alice", 42.0));
CompletableFuture<WorkflowModel> result = instance.start();
// ... publish a PAYMENT_RECEIVED CloudEvent carrying flowinstanceid = instance.id() ...
// ... then a SHIPMENT_DISPATCHED CloudEvent ...
OrderResult out = result.get().as(OrderResult.class).orElseThrow();  // "DELIVERED"
```

The transformation is mechanical:

1. **States become positions.** `AWAITING_PAYMENT` and `AWAITING_SHIPMENT` are `listen` tasks the instance parks on. The terminal `DELIVERED`/`CANCELLED` states become `.then(END)`.
2. **Events become correlated CloudEvents.** `sendEvent(PAYMENT_RECEIVED)` becomes "publish a `org.acme.order.payment.received` CloudEvent carrying the instance id." The engine correlates it to the parked instance via the `flowinstanceid` extension.
3. **Guards become a `switch`.** The approved/rejected guard pair collapses into one `switchWhenOrElse`.
4. **Actions become call tasks.** Each Spring `Action` bean becomes a `function(...)` task calling a plain method — no framework callback interface required, and each method is unit-testable on its own.
5. **Starting the instance is the initial transition.** There's no separate `PLACE` event; starting the instance with the `OrderRequest` is the trigger.

> The full, runnable example (REST endpoints, CloudEvent wiring, and tests that run without Docker) lives in the Quarkus Flow repo under [`examples/spring-statemachine-migration`](https://github.com/quarkiverse/quarkus-flow/tree/main/examples/spring-statemachine-migration).

### Mapping the Pieces in Code

A few patterns are worth seeing in isolation.

**Guard → `switch`.** A Spring `Guard<S,E>` that returns a boolean becomes a routing predicate:

```java
// Spring: .guard(ctx -> payment.approved())
// Flow:
switchWhenOrElse(PaymentEvent::approved, "fulfillOrder", "cancelOrder", PaymentEvent.class)
```

For N-way (junction) branching, use explicit cases:

```java
switchCase("route",
    caseOf(o -> o.amount() > 1000, "manualReview"),
    caseOf(o -> o.amount() > 0,   "autoApprove"),
    caseDefault("reject"))
```

**Action → call task.** This is the headline difference. A Spring action is code bolted onto a state or transition, implementing a framework interface. In Quarkus Flow it is a first-class task calling an ordinary method:

```java
function("fulfillOrder", this::fulfillOrder, PaymentEvent.class)
```

That method has no framework coupling, is reusable, and is trivially unit-testable in isolation.

**Fork / join → `fork`.** Spring regions (orthogonal states) become concurrent branches that rejoin by default:

```java
fork("fulfil",
    function("reserveStock",    this::reserveStock,    OrderRequest.class),
    function("chargeShipping",  this::chargeShipping,  OrderRequest.class))
// all branches complete and merge; use .compete(true) for first-wins
```

**Composite state → `subflow`.** A hierarchical state that contains its own machine becomes a reusable sub-workflow:

```java
// workflow(namespace, name, version) — namespaces follow Kubernetes DNS
// naming (lowercase, hyphens — no dots)
subflow("runFulfilment", workflow("org-acme", "fulfilment-workflow", "1.0"))
```

**Self / loop transition → `.then(...)`.** Any task can route to any named task, including backwards, so cycles and back-transitions are a single uniform mechanism — for example, sending an order back to its waiting state after a payment mismatch:

```java
function("flagPaymentMismatch", this::flagMismatch, PaymentEvent.class)
    .then("awaitPayment")   // cycle back to the waiting state
```

Don't reach for this to implement **retries**, though. The Open Workflow Specification has first-class retry policies (constant, linear, and exponential backoff) and a `try`/`catch` task, so a flaky call is *declared* resilient rather than wired into a hand-rolled loop. Reserve `.then(...)` cycles for genuine business back-transitions.

**Extended state → `set` / `withContext`.** Store data that later tasks and guards can read:

```java
set("currentState", "PAID")                 // write a field
// later, read it in a context-aware task; the lambda receives the
// task input first, then the workflow context:
withContext((p, ctx) -> enrich(p, ctx))
```

### "What State Am I In?"

This is the first question every Spring StateMachine user asks, because `machine.getState()` is so central. Quarkus Flow answers it on two axes.

**Lifecycle status** — the coarse-grained answer — comes straight off the instance:

```java
WorkflowStatus status = instance.status();
// PENDING, RUNNING, WAITING, COMPLETED, FAULTED, CANCELLED, SUSPENDED
```

**Current task** — the fine-grained "which state" answer — comes from a `WorkflowExecutionListener`, the direct analog of Spring's `StateMachineListener`. Register it as a CDI bean and the engine discovers it automatically:

```java
@ApplicationScoped
public class StateTracker implements WorkflowExecutionListener {

    private static final Logger LOG = LoggerFactory.getLogger(StateTracker.class);

    @Override
    public void onTaskStarted(TaskStartedEvent ev) {         // ~ stateEntered
        LOG.info("Instance {} entered state '{}'",
            ev.workflowContext().instanceId(),
            ev.taskContext().taskName());
    }

    @Override
    public void onTaskCompleted(TaskCompletedEvent ev) {     // ~ stateExited
        LOG.info("Instance {} left state '{}'",
            ev.workflowContext().instanceId(),
            ev.taskContext().taskName());
    }
}
```

`taskContext().taskName()` is your current "state" name, and `position()` gives the precise location. If you prefer an explicitly queryable field (closest to `getState()`), write one into the workflow data with `set("currentState", ...)` at each step and read it back from the instance's context.

And if you only want to *observe* transitions without writing a listener, just turn on tracing in `application.properties`:

```properties
quarkus.flow.tracing.enabled=true
```

Every task start/completion then shows up in your traces out of the box.

### Durability and Lifecycle, When You Want Them

A Spring StateMachine lives in memory, and persisting/restoring it is your job via `StateMachinePersist`. Quarkus Flow runs in memory too **by default** — the instance executes and is discarded, same as an FSM. The difference is that making it durable is a dependency plus a config toggle, not code you write.

**Opt-in persistence that resumes at the exact state.** Add a persistence backend (Redis, JPA, or embedded MVStore) and an instance parked at `awaitPayment` survives a JVM restart — resuming *parked at `awaitPayment`*, waiting for the same event. The engine records the next position alongside the data and restores it on boot, so a multi-day wait for a payment callback stops being something you engineer and becomes a backend you plug in. Leave persistence out and the instance behaves exactly like an in-memory FSM: it runs, completes, and is gone. See the [`durable-workflows-k8s`](https://github.com/quarkiverse/quarkus-flow/tree/main/examples/durable-workflows-k8s) example.

**Persistence without the Kryo attack surface.** How a machine's state is serialized is a security concern, not just an implementation detail. Spring StateMachine's persistence backends serialize the machine context with [Kryo](https://github.com/EsotericSoftware/kryo), and [CVE-2026-41862](https://spring.io/security/cve-2026-41862/) (CVSS 8.8, High) showed the cost: those backends deserialized a persisted context *without a class allowlist*, so anyone able to write to the store — a JPA row, a Redis value, a ZooKeeper znode — could trigger remote code execution inside the JVM. The fix enables Kryo registration enforcement, a backward-incompatible change that forces every custom state/event type to be registered explicitly; and the end-of-life `3.2.x` line has no public open-source fix at all. Quarkus Flow doesn't carry that surface: it marshals the workflow model through the Open Workflow Java SDK, serializing data as JSON (Jackson) and reading it back into the *declared target type* rather than instantiating whatever class a payload happens to name. It's a narrower, more predictable attack surface — and one more reason a maintained, open foundation matters.

**Suspend / resume / cancel** are built into the instance API — something Spring StateMachine only approximates through persistence:

```java
instance.suspend();   // pause a running instance
instance.resume();    // continue where it left off
instance.cancel();    // terminate it
```

See the [`suspend-resume-abort`](https://github.com/quarkiverse/quarkus-flow/tree/main/examples/suspend-resume-abort) example.

**Retries and backoff** are declarative, with constant/linear/exponential strategies — no `onError` plumbing to write by hand.

### Common Questions

**Do I lose explicit, enumerated states?**
Conceptually, yes — Flow is task-positional, not state-declared. In practice you recover the same visibility through `status()`, the current task name via a listener, or an explicit `set("currentState", ...)` field. For most applications this is *more* observable than a bare FSM, because it's integrated with tracing and metrics.

**How do I query the current state at any moment?**
`instance.status()` for lifecycle; a `WorkflowExecutionListener` (or an explicit `currentState` field) for the specific step. See the section above.

**What about history states?**
There's no dedicated shallow/deep history pseudostate. For the common case — a *live* instance resuming where it left off — durable persistence does exactly that: a parked instance continues at the position it was waiting on, no history node required. To re-enter an earlier point within a *still-running* instance, use a `.then(...)` back-transition with a marker in the context that records where to resume. And if you want a record of the path an instance took, a `WorkflowExecutionListener` can persist each `onTaskStarted`/`onTaskCompleted` to a store of your choice. One boundary to keep in mind: once an instance reaches a terminal state it cannot be re-executed — so this is about resuming and recording live instances, not replaying finished ones.

**Is everything asynchronous now? I relied on synchronous `sendEvent`.**
Only the steps that wait on external events are. A workflow built from `function`/`switch`/`set` tasks runs straight through to completion, and `start()` returns a future you can `await` immediately. It's the `listen`/`wait` tasks — your event-driven "waiting states" — that make an instance park asynchronously, which is exactly what you want for a payment callback or a human approval. If you need microsecond-latency synchronous transitions with no I/O on a hot path, an in-process FSM still wins — see "Is Quarkus Flow right for you?".

**Can I still unit-test my transitions and actions?**
Yes, and more easily. Because actions are plain methods, you unit-test them in isolation. End-to-end, `@QuarkusTest` drives the whole workflow; you can swap Kafka for the in-memory connector so the suite runs without Docker. The example's tests do exactly that.

**What about hierarchical / composite machines?**
Extract the substate machine into its own workflow and invoke it with `subflow(...)`. Parent/child composition is first-class.

**Do I have to write Java DSL, or can I use YAML?**
Both. This guide uses the Java DSL because Spring StateMachine users configure machines in code, but every workflow can also be authored in YAML/JSON — and the [Runner](https://docs.quarkiverse.io/quarkus-flow/dev/runner.html) extension can serve YAML workflows with zero application code.

### Getting Started

1. Read the [Quarkus Flow docs](https://docs.quarkiverse.io/quarkus-flow/dev/) and skim the [Open Workflow Specification](https://github.com/open-workflow-specification/specification).
2. Clone the runnable migration example: [`examples/spring-statemachine-migration`](https://github.com/quarkiverse/quarkus-flow/tree/main/examples/spring-statemachine-migration). It models this exact order lifecycle, exposes REST endpoints to drive it, and ships tests that run without Docker.
3. Inventory your Spring StateMachine: list its states, events, guards, and actions, then use the concept-mapping table above to translate them task-by-task.
4. Decide per machine whether it belongs in Flow at all (the "is this right for you?" test). Move the event-driven, durable, distributed ones first.

Spring StateMachine served the Java community well, and its `4.0.x` line remains available. But if you need a *maintained*, Apache-2.0 foundation built on an open **CNCF specification** — one no single vendor can close or relicense — and especially if your state machines have grown into event-driven, long-running, distributed orchestrations, Quarkus Flow is a natural home, with durability, observability, and AI orchestration built in rather than bolted on.

**Resources:**

- [Quarkus Flow on GitHub](https://github.com/quarkiverse/quarkus-flow)
- [Quarkus Flow documentation](https://docs.quarkiverse.io/quarkus-flow/dev/)
- [Spring StateMachine migration example](https://github.com/quarkiverse/quarkus-flow/tree/main/examples/spring-statemachine-migration)
- [Open Workflow Specification](https://github.com/open-workflow-specification/specification)
- [Custom execution listeners](https://docs.quarkiverse.io/quarkus-flow/dev/custom-listeners.html)
- [Spring's end-of-open-source announcement (April 2025)](https://spring.io/blog/2025/04/21/spring-cloud-data-flow-commercial/)
