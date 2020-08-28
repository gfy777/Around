package com.around;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class PostDumpFlow {

    private static final String PROJECT_ID = "orbital-heaven-286400";
    private static final Charset UTF8_CHARSET = StandardCharsets.UTF_8;


    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();

        Pipeline p = Pipeline.create(options);

        CloudBigtableScanConfiguration configuration = new CloudBigtableScanConfiguration.Builder()
                .withProjectId(PROJECT_ID)
                .withInstanceId("around-post")
                .withTableId("post")
                .build();

        PCollection<Result> btRows = p.apply(Read.from(CloudBigtableIO.read(configuration)));
        PCollection<TableRow> bqRows = btRows.apply(ParDo.of(new DoFn<Result, TableRow>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                Result result = c.element();
                String postId = new String(result.getRow());
                String user = new String(result.getValue(Bytes.toBytes("post"), Bytes.toBytes("user")), UTF8_CHARSET);
                String message = new String(result.getValue(Bytes.toBytes("post"), Bytes.toBytes("message")), UTF8_CHARSET);
                String lat = new String(result.getValue(Bytes.toBytes("location"), Bytes.toBytes("lat")), UTF8_CHARSET);
                String lon = new String(result.getValue(Bytes.toBytes("location"), Bytes.toBytes("lon")), UTF8_CHARSET);

                TableRow row = new TableRow();
                row.set("postId", postId);
                row.set("user", user);
                row.set("message", message);
                row.set("lat", Double.parseDouble(lat));
                row.set("lon", Double.parseDouble(lon));
                c.output(row);
            }
        }));

        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("postId").setType("STRING"));
        fields.add(new TableFieldSchema().setName("user").setType("STRING"));
        fields.add(new TableFieldSchema().setName("message").setType("STRING"));
        fields.add(new TableFieldSchema().setName("lat").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("lon").setType("FLOAT"));

        TableSchema schema = new TableSchema().setFields(fields);
        bqRows.apply(BigQueryIO.writeTableRows()
                .to(PROJECT_ID+":"+"post_analysis"+"."+"daily_dump_1")
                .withSchema(schema)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        p.run();
    }
}

// Maven command to start service
// mvn -Pdataflow-runner compile exec:java -Dexec.mainClass=com.around.PostDumpFlow -Dexec.args="--project=orbital-heaven-286400 --stagingLocation=gs://dataflow-around-rg/staging/ --tempLocation=gs://dataflow-around-rg/output --runner=DataflowRunner --jobName=dataflow-intro"