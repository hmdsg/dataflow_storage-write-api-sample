package com.example.dataflow;

import java.util.List;
import java.util.ArrayList;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class BqTest {
    public static void main (String args[]){
        System.out.println("start");
        execPipeline();
    }

    private static TestStream<String> createEventSource() {
        Instant startTime = new Instant(0);
        return TestStream.create(StringUtf8Coder.of())
                .advanceWatermarkTo(startTime)
                .addElements(
                        TimestampedValue.of("Alice,20", startTime),
                        TimestampedValue.of("Bob,30",
                                startTime.plus(Duration.standardSeconds(1))),
                        TimestampedValue.of("Charles,40",
                                startTime.plus(Duration.standardSeconds(2))),
                        TimestampedValue.of("Dylan,Invalid value",
                                startTime.plus(Duration.standardSeconds(2))))
                .advanceWatermarkToInfinity();
    }

    private static PipelineResult execPipeline() {
        // Define schema
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("user_name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("age").setType("STRING"));
        TableSchema schema = new TableSchema().setFields(fields);

        PipelineOptionsFactory.register(ExamplePipelineOptions.class);
        ExamplePipelineOptions options = PipelineOptionsFactory.fromArgs()
                .as(ExamplePipelineOptions.class);
        options.setStreaming(true);

        // Create a pipeline and apply transforms.
        Pipeline pipeline = Pipeline.create(options);
        pipeline
                .apply(createEventSource())
                .apply(MapElements
                        .into(TypeDescriptor.of(TableRow.class))
                        .via((String x) -> {
                            String[] columns = x.split(",");
                            return new TableRow().set("user_name", columns[0]).set("age", columns[1]);
                        }))
                // Write the rows to BigQuery
                .apply(BigQueryIO.writeTableRows()
                        .to(String.format("%s:%s.%s",
                                "$PROJECT_ID",
                                "$DATASET_NAME",
                                "$TABLE_NAME"))
                        .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                        .withSchema(schema)
                        .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                        .withMethod(Write.Method.STORAGE_WRITE_API)
                        .withExtendedErrorInfo()
                        .withTriggeringFrequency(Duration.standardSeconds(5)))
                // Get the collection of write errors.
                .getFailedStorageApiInserts()
                .apply(MapElements.into(TypeDescriptors.strings())
                        // Process each error. In production systems, it's useful to write the errors to
                        // another destination, such as a dead-letter table or queue.
                        .via(
                                x -> {
                                    System.out.println("Failed insert: " + x.getErrorMessage());
                                    System.out.println("Row: " + x.getRow());
                                    return "";
                                }));
        return pipeline.run();
    }
}
