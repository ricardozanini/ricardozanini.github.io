---
title: "What are workflows and why do I need them?"
draft: false
date: 2026-05-14
tags: ["worflows", "java", "quarkus flow"]
categories: ["Engineering", "Workflow Patterns"]
---

Imagine you have to design a system that would provision a Kubernetes namespace for users after processing and accepting payment. 
The flow is quite simple:

1. A new user is created on Auth0
2. The user is billed via Stripe API
3. The system reserve a new namespace on Kubernetes in the company's infrastructure.

In a scenario where this sequence fails on the last step, the system now has to revert the billed user and change their role on the authz database.

Your code might look like this:

```java
// Pseudocode for the cloud company namespace reservation
public class CoordinateCloudNamespace {

    @Inject
    private Auth0Service auth0Service;
    @Inject
    private StripeClientService stripeService;
    @Inject
    private AWSCloudK8sService kubeService;

    public CloudSpace reserveCloud(User user, PaymentToken payment, Namespace namespace) {

        var authzUser;
        try {
            authzUser = auth0Service.createUser(user, namespace);
        } catch (Auth0Exception ex) {
            throw new RuntimeException(ex);
        }

        try {

        } catch ()
        // ... etc

    }
}
```

Soon, you realize that your team will struggle to keep up and maintain this code in a health maintainable state duo to the increase of rules and compensation complexity that will you have to add.


Academia defines workflows as "The automation of a business process, in whole or part, during which documents, information or
tasks are passed from one participant to another for action, according to a set of procedural rules." [ref]. 
This definition can be simplified and generalized as "the automatition of a process, where data are passed from one task to another for execution, following a set of rules."

So anything in your code base can be classified as a workflow. It's the way your functions are defined to route structured data during execution of a case.

Despite the complexity of your system, you might have hard-coded a full workflow management system:

1. Functions (tasks) are **defined** in a way to support a required case.
2. **Data** is **routed** and modified during the functions execution following a strict set of **rules**.
3. **Tasks** can be executed by humans (your system actors) and/or non-humans (your methods, a remote service, etc.).
4. A record in your database resulted from an **execution** is an **instance**.

For cases where your functions are bounded to a well-defined local boundaries, following the best programming practices, you shouldn't have too many issues in the long term.

Challenges may arise in the moment the system must interact externally, be part of a choreography or orchestrate complex executions. To keep the system sane and maintainable (even in the AI era), you should consider a workflow management system.

=== Presenting Quarkus Flow

The [CNCF Serverless Workfow specification](Add URL) describes a workflow definition language for workflow runtimes. The project's goal is to keep a vendor-neutral, open source language that any multi-language systems can understand and run. 

[Quarkus Flow]() is a workflow runtime library built on top of [Quarkus](Add URL) to enhance **any** Quarkus application with a workflow engine. This means that your service can run a tiny workflow management system... 

<!--TODO: continue justification of quarkus flow -->

<!--TODO: Exemplify the same example using Quarkus Flow Java DSL -->

```java
//- TODO: Add a Quarkus Flow Java DSL reimplementing the same pseudocode and highlight the approach of the code now being imperative.
public class CoordinateCloudNamespaceWorkflow extends Flow {

    @Inject
    private Auth0Service auth0Service;
    @Inject
    private StripeClientService stripeService;
    @Inject
    private AWSCloudK8sService kubeService;

    @Override
    public Workflow descriptor() {

        // similar example in https://github.com/quarkiverse/quarkus-flow/blob/main/examples/order-fulfillment-compensation/src/main/java/org/acme/flow/OrderFulfillmentWorkflow.java

    }

}
```

- Conclusion: get back the original problem in a gist, present the solution for the micro Java system world and explain how workflow patterns actually helps solving orchestration problems leaning back to workflow patterns.