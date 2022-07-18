# -*- coding: utf-8 -*-
"""
Created on Thu Jun 30 15:23:55 2022

@author: angus
"""
#!/usr/bin/env python
import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

INPUT_SUBSCRIPTION = "projects/testproject-354901/subscriptions/shipment-sub"
BIGQUERY_TABLE = "testproject-354901:order_tracking.order_tracking_custom_job"
BIGQUERY_SCHEMA = "timestamp:TIMESTAMP,sensor_id:STRING,datetime:STRING,timezone:STRING,longitude:STRING,latitude:STRING,order_id:STRING,vehicle_id:STRING"

class CustomParsing(beam.DoFn):
    """ Custom ParallelDo class to apply a custom transformation """

    def process(self, element: bytes, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam):
        """
        Simple processing function to parse the data and add a timestamp
        For additional params see:
        https://beam.apache.org/releases/pydoc/2.7.0/apache_beam.transforms.core.html#apache_beam.transforms.core.DoFn
        """
        parsed = json.loads(element.decode("utf-8"))
        parsed["timestamp"] = timestamp.to_rfc3339()
        yield parsed

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_subscription",
        help='Input PubSub subscription of the form "projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."',
        default=INPUT_SUBSCRIPTION,
    )
    parser.add_argument(
        "--output_table", help="Output BigQuery Table", default=BIGQUERY_TABLE
    )
    parser.add_argument(
        "--output_schema",
        help="Output BigQuery Schema in text format",
        default=BIGQUERY_SCHEMA,
    )
    known_args, pipeline_args = parser.parse_known_args()

    # Creating pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    # Defining our pipeline and its steps
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadFromPubSub" >> beam.io.gcp.pubsub.ReadFromPubSub(
                subscription=known_args.input_subscription, timestamp_attribute=None
            )
            | "CustomParse" >> beam.ParDo(CustomParsing())
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                known_args.output_table,
                schema=known_args.output_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == "__main__":
    run()
    
    
# python dataflow_streaming_pipeline.py --job_name dataflow-custom-streaming-pipeline --project=testproject-354901 --runner=DataflowRunner --region=us-central1 --streaming --staging_location=gs://testproject-354901/temp --temp_location gs://testproject-354901/temp