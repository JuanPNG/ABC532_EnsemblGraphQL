from typing import Any

import json

import requests


GRAPHQL_URL = 'https://beta.ensembl.org/data/graphql'
REQUEST_TIMEOUT = 30


class EnsemblApiError(RuntimeError):
    """Raised when the Ensembl API returns an unexpected response."""


def _response_json(response: requests.Response) -> dict[str, Any]:
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise EnsemblApiError(
            f'Ensembl request failed with HTTP {response.status_code}: {response.text}'
        ) from exc

    try:
        payload = response.json()
    except ValueError as exc:
        raise EnsemblApiError('Ensembl response was not valid JSON.') from exc

    if not isinstance(payload, dict):
        raise EnsemblApiError('Ensembl response JSON was not an object.')

    return payload


def _request_json(method: str, url: str, **kwargs: Any) -> dict[str, Any]:
    try:
        response = requests.request(method, url, timeout=REQUEST_TIMEOUT, **kwargs)
    except requests.RequestException as exc:
        raise EnsemblApiError(f'Ensembl request failed: {exc}') from exc

    return _response_json(response)


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
        GRAPHQL_URL,
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
            f'Expected one genome for accession {genome_accession}, found {len(genomes)}.'
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


if __name__ == '__main__':
    stats = retrieve_genome_stats(genome_accession='GCA_964271515.3')

    print(json.dumps(stats, indent=2))

# Results:

# stats = {'genome_stats': {'assembly_stats': {'contig_n50': 2037218, 'total_genome_length': 1318353577,
#                                            'total_coding_sequence_length': 18979379, 'total_gap_length': 109500,
#                                            'spanned_gaps': 1070, 'chromosomes': 32, 'toplevel_sequences': 65,
#                                            'component_sequences': 65, 'gc_percentage': 41.45},
#                         'coding_stats': {'coding_genes': 12457, 'average_genomic_span': 23148.89,
#                                          'average_sequence_length': 1571.58, 'average_cds_length': 1377.02,
#                                          'shortest_gene_length': 84, 'longest_gene_length': 471568,
#                                          'total_transcripts': 19968, 'coding_transcripts': 19968,
#                                          'transcripts_per_gene': 1.6, 'coding_transcripts_per_gene': 1.6,
#                                          'total_exons': 166061, 'total_coding_exons': 165821,
#                                          'average_exon_length': 170.76, 'average_coding_exon_length': 165.82,
#                                          'average_exons_per_transcript': None,
#                                          'average_coding_exons_per_coding_transcript': None, 'total_introns': 146093,
#                                          'average_intron_length': 2591.39},
#                         'variation_stats': {'short_variants': None, 'structural_variants': None,
#                                             'short_variants_with_phenotype_assertions': None,
#                                             'short_variants_with_publications': None,
#                                             'short_variants_frequency_studies': None,
#                                             'structural_variants_with_phenotype_assertions': None},
#                         'non_coding_stats': {'non_coding_genes': 7062, 'small_non_coding_genes': 3269,
#                                              'long_non_coding_genes': 3294, 'misc_non_coding_genes': 499,
#                                              'average_genomic_span': 1805.6, 'average_sequence_length': 287.36,
#                                              'shortest_gene_length': 42, 'longest_gene_length': 187172,
#                                              'total_transcripts': 7973, 'transcripts_per_gene': 1.13,
#                                              'total_exons': 14409, 'average_exon_length': 178.07,
#                                              'average_exons_per_transcript': 1.81, 'total_introns': 6436,
#                                              'average_intron_length': 1928.99},
#                         'pseudogene_stats': {'pseudogenes': 0, 'average_genomic_span': None,
#                                              'average_sequence_length': None, 'shortest_gene_length': None,
#                                              'longest_gene_length': None, 'total_transcripts': None,
#                                              'transcripts_per_gene': None, 'total_exons': None,
#                                              'average_exon_length': None, 'average_exons_per_transcript': None,
#                                              'total_introns': None, 'average_intron_length': None},
#                         'homology_stats': {'coverage': 53.9,
#                                            'reference_species_name': 'pomacea_canaliculata_gca003073045v1rs'},
#                         'regulation_stats': {'enhancers': None, 'promoters': None, 'ctcf_count': None,
#                                              'tfbs_count': None, 'open_chromatin_count': None}}}

# https://beta.ensembl.org/api/metadata/genome/2e19b017-6548-4d20-a317-e6b501c890ca/stats
