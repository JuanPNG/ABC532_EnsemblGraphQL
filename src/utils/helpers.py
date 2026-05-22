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


class EnaApiError(RuntimeError):
    """Raised when the ENA API returns an unexpected response."""


ENA_REQUEST_TIMEOUT_SECONDS = 30
ENA_MAX_REQUEST_ATTEMPTS = 5
ENA_DEFAULT_RETRY_AFTER_SECONDS = 10
ENA_RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}

ENA_TAG_TO_KEY = {
    'ungapped-length': 'ungapped_length',
    'n50': 'scaffold_n50',
    'scaffold-count': 'scaffold_count',
    'count-contig': 'contig_count',
    'contig-n50': 'contig_n50',
    'contig-L50': 'contig_l50',
    'contig-n75': 'contig_n75',
    'contig-n90': 'contig_n90',
    'scaf-L50': 'scaffold_l50',
    'scaf-n75': 'scaffold_n75',
    'scaf-n90': 'scaffold_n90',
    'spanned-gaps': 'spanned_gaps',
    'unspanned-gaps': 'unspanned_gaps',
    'replicon-count': 'replicon_count',
    'count-non-chromosome-replicon': 'non_chromosome_replicon_count',
}

ENA_INTEGER_METRIC_KEYS = {
    'ungapped_length',
    'scaffold_n50',
    'scaffold_count',
    'contig_n50',
    'contig_count',
    'spanned_gaps',
    'unspanned_gaps',
    'contig_l50',
    'scaffold_l50',
    'contig_n75',
    'contig_n90',
    'scaffold_n75',
    'scaffold_n90',
    'replicon_count',
    'non_chromosome_replicon_count',
}

ENA_ASSEMBLY_METRIC_KEYS = [
    'assembly_level',
    'ungapped_length',
    'scaffold_n50',
    'scaffold_count',
    'contig_n50',
    'contig_count',
    'coverage',
    'spanned_gaps',
    'unspanned_gaps',
    'contig_l50',
    'scaffold_l50',
    'contig_n75',
    'contig_n90',
    'scaffold_n75',
    'scaffold_n90',
    'replicon_count',
    'non_chromosome_replicon_count',
]


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


def _request_ena_json(method: str, url: str, **kwargs: Any) -> dict[str, Any]:
    def retry_delay_seconds(
        attempt: int,
        response: requests.Response | None = None,
    ) -> float:
        if response is not None:
            retry_after = response.headers.get('Retry-After')
            if retry_after:
                return parse_retry_after(retry_after)

        return min(ENA_DEFAULT_RETRY_AFTER_SECONDS, attempt * 2)

    def parse_retry_after(retry_after: str) -> float:
        try:
            return max(float(retry_after), 0)
        except ValueError:
            try:
                parsed_date = parsedate_to_datetime(retry_after)
            except (TypeError, ValueError):
                return ENA_DEFAULT_RETRY_AFTER_SECONDS

            return max((parsed_date.timestamp() - time.time()), 0)

    last_response = None

    for attempt in range(1, ENA_MAX_REQUEST_ATTEMPTS + 1):
        try:
            response = requests.request(
                method,
                url,
                timeout=ENA_REQUEST_TIMEOUT_SECONDS,
                **kwargs,
            )
        except requests.RequestException as exc:
            if attempt == ENA_MAX_REQUEST_ATTEMPTS:
                raise EnaApiError(f'ENA request failed: {exc}') from exc

            time.sleep(retry_delay_seconds(attempt=attempt))
            continue

        last_response = response

        if response.status_code in ENA_RETRYABLE_STATUS_CODES:
            if attempt == ENA_MAX_REQUEST_ATTEMPTS:
                break

            time.sleep(retry_delay_seconds(attempt=attempt, response=response))
            continue

        try:
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as exc:
            raise EnaApiError(f'ENA request failed: {exc}') from exc
        except ValueError as exc:
            raise EnaApiError('ENA response was not valid JSON.') from exc

        if not isinstance(payload, dict):
            raise EnaApiError('ENA response JSON was not an object.')

        return payload

    raise EnaApiError(
        f'ENA request failed after {ENA_MAX_REQUEST_ATTEMPTS} attempts with '
        f'HTTP {last_response.status_code}: {last_response.text}'
    )


def fetch_ena_assembly(accession: str) -> dict[str, Any]:
    """Fetch a single ENA assembly summary record for an accession."""
    payload = _request_ena_json('GET', f'https://www.ebi.ac.uk/ena/browser/api/summary/{accession}')

    summaries = payload.get('summaries')
    if not isinstance(summaries, list):
        raise EnaApiError('ENA summary response did not include summaries list.')

    if not summaries:
        raise EnaApiError(f'No ENA summaries returned for accession {accession}.')

    record = summaries[0]
    if not isinstance(record, dict):
        raise EnaApiError(f'ENA summary record was not an object: {record}')

    accession_root = accession.split('.')[0]
    returned_accession = record.get('accession')
    if returned_accession not in (None, accession, accession_root):
        raise EnaApiError(
            f'ENA returned accession {returned_accession} for requested {accession}.'
        )

    return record


def retrieve_ena_assembly_stats(accession: str) -> dict[str, Any]:
    """Fetch ENA assembly metrics as a flat BigQuery-ready record."""
    def _coerce_to_integer(key: str, value: Any) -> int | str | None:
        if key not in ENA_INTEGER_METRIC_KEYS:
            return value

        if value in (None, ''):
            return None

        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise EnaApiError(
                f'Could not parse ENA metric {key}={value!r} as integer.'
            ) from exc

    def _coerce_to_float(key: str, value: Any) -> float | None:
        if value in (None, ''):
            return None

        try:
            return float(value)
        except (TypeError, ValueError) as exc:
            raise EnaApiError(
                f'Could not parse ENA metric {key}={value!r} as float.'
            ) from exc

    record = fetch_ena_assembly(accession)
    metrics = {key: None for key in ENA_ASSEMBLY_METRIC_KEYS}

    assembly_level = record.get('assemblyLevel')
    if isinstance(assembly_level, str):
        metrics['assembly_level'] = assembly_level.strip().lower()
    elif assembly_level is not None:
        metrics['assembly_level'] = assembly_level

    coverage = record.get('assemblyCoverage')
    metrics['coverage'] = _coerce_to_float('coverage', coverage)

    attributes = record.get('attributes') or []
    if not isinstance(attributes, list):
        raise EnaApiError('ENA summary attributes was not a list.')

    for attribute in attributes:
        if not isinstance(attribute, dict):
            raise EnaApiError(f'ENA summary attribute was not an object: {attribute}')

        tag = attribute.get('tag')
        key = ENA_TAG_TO_KEY.get(tag)
        if key is None:
            continue

        metrics[key] = _coerce_to_integer(key, attribute.get('value'))

    return {
        'accession': accession,
        **metrics,
    }


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
