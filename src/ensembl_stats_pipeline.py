import argparse
import json

import apache_beam as beam
from apache_beam.io import BigQueryDisposition
from apache_beam.io.filesystems import FileSystems

from utils.helpers import convert_dict_to_table_schema
from utils.transforms import (
    RetrieveGenomeStatsDoFn,
)


def ensembl_stats_pipeline(args, beam_args) -> None:

    options = beam.pipeline.PipelineOptions(beam_args)

    with beam.Pipeline(options=options) as p:
        inputs = (
                p
                | 'ReadTaxonomyJSONL' >> beam.io.ReadFromText(args.accessions_file)
        )

        retrieved_stats = (
            inputs
            | 'RetrieveEnsemblGenomeStats' >> beam.ParDo(
                RetrieveGenomeStatsDoFn(
                    api_call_delay_seconds=args.ensembl_api_delay_seconds,
                ),
            ).with_outputs("errors", main='stats')
        )

        if args.bq_table and args.bq_schema and args.temp_location:

            with FileSystems.open(args.bq_schema) as f:
                schema_dict = json.load(f)
                table_schema = convert_dict_to_table_schema(schema_dict)

            (
                retrieved_stats.stats
                | 'WriteGenomeStatsBigQuery' >> beam.io.WriteToBigQuery(
                    table=args.bq_table,
                    schema=table_schema,
                    method='FILE_LOADS',
                    custom_gcs_temp_location=args.temp_location,
                    write_disposition=BigQueryDisposition.WRITE_APPEND,
                    create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
                )
            )

        if args.output_jsonl:
            (
                retrieved_stats.stats
                | 'FormatStatsJSON' >> beam.Map(json.dumps)
                | 'WriteStatsJSONL' >> beam.io.WriteToText(
                    args.output_jsonl,
                    file_name_suffix='.jsonl',
                    shard_name_template='',
                )
            )
        elif not args.bq_table:
            (
                retrieved_stats.stats
                | 'FormatStatsForPrint' >> beam.Map(json.dumps)
                | 'PrintStats' >> beam.Map(print)
            )

        if args.errors_jsonl:
            (
                retrieved_stats.errors
                | 'FormatErrorsJSON' >> beam.Map(json.dumps)
                | 'WriteErrorsJSONL' >> beam.io.WriteToText(
                    args.errors_jsonl,
                    file_name_suffix='.jsonl',
                    shard_name_template='',
                )
            )
        else:
            (
                retrieved_stats.errors
                | 'FormatErrorsForPrint' >> beam.Map(json.dumps)
                | 'PrintErrors' >> beam.Map(print)
            )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--accessions_file',
        help='Path to a newline-delimited list of assembly accession IDs.', required=True,
    )
    parser.add_argument(
        '--output_jsonl',
        help='Optional path prefix for successful JSONL output.',
    )
    parser.add_argument(
        '--errors_jsonl',
        help='Optional path prefix for error JSONL output.',
    )
    parser.add_argument(
        '--bq_table',
        help='Optional BigQuery table destination: PROJECT:DATASET.TABLE or DATASET.TABLE.',
    )
    parser.add_argument(
        '--bq_schema',
        help='Path to the BigQuery JSON schema file.',
    )
    parser.add_argument(
        "--temp_location",
        help="The GCS path to store temporary BigQuery files.",
    )
    parser.add_argument(
        '--ensembl_api_delay_seconds',
        default=0.0,
        type=float,
        help='Seconds to sleep before each Ensembl API request per worker.',
    )

    args, beam_args = parser.parse_known_args()
    ensembl_stats_pipeline(args, beam_args)
