import json
import time
from email.utils import parsedate_to_datetime
from typing import Any

import requests

from apache_beam.io.gcp.internal.clients import bigquery as bq


class EnsemblApiError(RuntimeError):
    """Raised when the Ensembl API returns an unexpected response."""


ENSEMBL_REQUEST_TIMEOUT_SECONDS = 30
ENSEMBL_MAX_REQUEST_ATTEMPTS = 5
ENSEMBL_DEFAULT_RETRY_AFTER_SECONDS = 10
ENSEMBL_RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}


def _request_json(method: str, url: str, **kwargs: Any) -> dict[str, Any]:
    def retry_delay_seconds(
        attempt: int,
        response: requests.Response | None = None,
    ) -> float:
        if response is not None:
            retry_after = response.headers.get('Retry-After')
            if retry_after:
                return parse_retry_after(retry_after)

        return min(ENSEMBL_DEFAULT_RETRY_AFTER_SECONDS, attempt * 2)

    def parse_retry_after(retry_after: str) -> float:
        try:
            return max(float(retry_after), 0)
        except ValueError:
            try:
                parsed_date = parsedate_to_datetime(retry_after)
            except (TypeError, ValueError):
                return ENSEMBL_DEFAULT_RETRY_AFTER_SECONDS

            return max((parsed_date.timestamp() - time.time()), 0)

    last_response = None

    for attempt in range(1, ENSEMBL_MAX_REQUEST_ATTEMPTS + 1):
        try:
            response = requests.request(
                method,
                url,
                timeout=ENSEMBL_REQUEST_TIMEOUT_SECONDS,
                **kwargs,
            )
        except requests.RequestException as exc:
            if attempt == ENSEMBL_MAX_REQUEST_ATTEMPTS:
                raise EnsemblApiError(f'Ensembl request failed: {exc}') from exc

            time.sleep(retry_delay_seconds(attempt=attempt))
            continue

        last_response = response

        if response.status_code in ENSEMBL_RETRYABLE_STATUS_CODES:
            if attempt == ENSEMBL_MAX_REQUEST_ATTEMPTS:
                break

            time.sleep(retry_delay_seconds(attempt=attempt, response=response))
            continue

        try:
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as exc:
            raise EnsemblApiError(f'Ensembl request failed: {exc}') from exc
        except ValueError as exc:
            raise EnsemblApiError('Ensembl response was not valid JSON.') from exc

        if not isinstance(payload, dict):
            raise EnsemblApiError('Ensembl response JSON was not an object.')

        return payload

    raise EnsemblApiError(
        f'Ensembl request failed after {ENSEMBL_MAX_REQUEST_ATTEMPTS} attempts with '
        f'HTTP {last_response.status_code}: {last_response.text}'
    )


def retrieve_genome_id(genome_accession: str) -> str:
    genome_id_graphql_query = f'''query{{
      genomes(
        by_keyword: {{
          assembly_accession_id:{json.dumps(genome_accession)}
        }}) 
      {{
        genome_id
      }}
    }}'''

    payload = _request_json(
        'POST',
        'https://beta.ensembl.org/data/graphql',
        json={'query': genome_id_graphql_query},
    )

    if payload.get('errors'):
        raise EnsemblApiError(f'Ensembl GraphQL errors: {payload["errors"]}')

    try:
        data = payload['data']
        genomes = data['genomes']
    except (KeyError, TypeError) as exc:
        raise EnsemblApiError('Ensembl GraphQL response did not include data.genomes.') from exc

    if not isinstance(genomes, list):
        raise EnsemblApiError('Ensembl GraphQL data.genomes was not a list.')

    if not genomes:
        raise EnsemblApiError(f'No genome found for accession {genome_accession}.')

    if len(genomes) > 1:
        raise EnsemblApiError(
            f'Expected one uuid for accession {genome_accession}, found {len(genomes)}.'
        )

    genome = genomes[0]
    if not isinstance(genome, dict):
        raise EnsemblApiError(f'Genome response was not an object: {genome}')

    genome_id = genome.get('genome_id')
    if not genome_id:
        raise EnsemblApiError(f'Genome response did not include genome_id: {genome}')

    return genome_id


def retrieve_genome_stats(genome_accession: str) -> dict[str, Any]:
    genome_id = retrieve_genome_id(genome_accession)

    return retrieve_genome_stats_by_id(genome_id)


def retrieve_genome_stats_by_id(genome_id: str) -> dict[str, Any]:
    ensembl_stats_url = f'https://beta.ensembl.org/api/metadata/genome/{genome_id}/stats'

    return _request_json('GET', ensembl_stats_url)


## From helpers all

def convert_dict_to_table_schema(schema_dict_list):
    """
    Converts a list of schema dicts (from JSON) into a Beam-compatible TableSchema.
    Recursively parse nested fields (Type: RECORD).
    """
    def _convert_field(field_dict):
        field = bq.TableFieldSchema()
        field.name = field_dict["name"]
        field.type = field_dict["type"]
        field.mode = field_dict.get("mode", "NULLABLE")

        if field.type == "RECORD" and "fields" in field_dict:
            field.fields.extend([_convert_field(f) for f in field_dict["fields"]])

        return field

    schema = bq.TableSchema()
    schema.fields.extend([_convert_field(f) for f in schema_dict_list])
    return schema
