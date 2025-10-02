package io.orkes.examples;

import com.netflix.conductor.sdk.workflow.task.InputParam;
import com.netflix.conductor.sdk.workflow.task.OutputParam;
import com.netflix.conductor.sdk.workflow.task.WorkerTask;

public class ExampleWorkers {
    public static class DatabaseRow {
        public String contents;
        public long lastUpdated;
    }

    public static class DatabaseQuery {
        public String query;
    }

    @WorkerTask("QueryDatabase")
    public @OutputParam("row") DatabaseRow queryDatabase(@InputParam("query") DatabaseQuery query) {
        var row = new DatabaseRow();

        row.contents = query.query;
        row.lastUpdated = System.currentTimeMillis();

        return row;
    }
}
