package io.orkes.examples;

import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.sdk.workflow.def.WorkflowBuilder;
import com.netflix.conductor.sdk.workflow.def.tasks.*;
import com.netflix.conductor.sdk.workflow.executor.WorkflowExecutor;
import io.orkes.conductor.client.ApiClient;

import java.io.IOException;
import java.util.UUID;

public class Main {
    static Task<?> createForkGenerationTask() {
        try (var script = Main.class.getClassLoader().getResourceAsStream("forkGenerator.js")) {
            if (script == null) {
                throw new RuntimeException("Script not found: forkGenerator.js");
            }

            var task = new Javascript("generateForkBranches", script);

            task.input("evaluatorType", "graaljs");
            task.input("forkCount", 100);

            return task;
        } catch (IOException e) {
            e.printStackTrace();

            throw new RuntimeException(e);
        }
    }

    static Task<?> createDynamicFork() {
        var task = new Task<>("forkOut", TaskType.FORK_JOIN_DYNAMIC) {};

        task.input("forkTaskWorkflow", "SimpleHTTPWorkflow");
        task.input("forkTaskInputs", "${generateForkBranches.output.result}");

        return task;
    }

    static Task<?> createDummyHTTPTask() {
        var task = new Http("http-" + UUID.randomUUID());

        task.name("http");
        task.url("https://orkes-api-tester.orkesconductor.com/api");
        task.method(Http.Input.HttpMethod.GET);

        return task;
    }

    static Task<?> createTwoBranchSwitch() {
        var task = new Switch("switch-" + UUID.randomUUID(), "");

        task.defaultCase(createDummyHTTPTask());
        task.switchCase("never", createDummyHTTPTask());

        return task;
    }

    static Task<?> createWaitFiveSeconds() {
        var task = new Wait("delay");

        task.input("duration", "5 seconds");

        return task;
    }

    static Task<?> createInnerLoopFork() {
        return new ForkJoin("innerLoopFork",
            new Task[] {
                createDummyHTTPTask(),
                createDummyHTTPTask()
            },
            new Task[] {
                createTwoBranchSwitch()
            }
        );
    }

    static Task<?> createLoop() {
        var task = new DoWhile("loop", 5,
            createInnerLoopFork()
        );

        return task;
    }

    public static void main(String[] args) {
        //Initialise Conductor Client
        var apiClient = new ApiClient(
                System.getenv("CONDUCTOR_SERVER_URL"),
                System.getenv("CONDUCTOR_ACCESS_KEY_ID"),
                System.getenv("CONDUCTOR_ACCESS_KEY_SECRET")
        );

        var executor = new WorkflowExecutor(apiClient, 10);
        var builder = new WorkflowBuilder<>(executor);

        var workflow = builder
            .name("example_workflow")
            .description("An example workflow that runs a simple task")
            .add(
                    createForkGenerationTask(),
                    createDynamicFork(),
                    new Join("joinForks"),
                    new ForkJoin("forks",
                            new Task[] {
                                    createTwoBranchSwitch(),
                            },
                            new Task[] {
                                    createWaitFiveSeconds()
                            },
                            new Task[] {
                                createLoop()
                            }
                    )
            )
            .version(1).build();

        if (executor.registerWorkflow(workflow.toWorkflowDef(), true)) {
            System.out.println("Workflow registered successfully: " + workflow.getName());
        } else {
            System.out.println("Failed to register workflow: " + workflow.getName());
        }

        executor.shutdown();
    }
}
