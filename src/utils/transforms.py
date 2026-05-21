import json
import time
from typing import Any, Iterable

import apache_beam as beam

from utils.helpers import EnsemblApiError, retrieve_genome_id, retrieve_genome_stats_by_id


class RetrieveGenomeStatsDoFn(beam.DoFn):
    """Resolve a taxonomy JSONL line or accession string to Ensembl stats."""

    def __init__(self, api_call_delay_seconds: float = 0.0) -> None:
        self.api_call_delay_seconds = api_call_delay_seconds

    def process(self, element: str) -> Iterable[dict[str, Any]]:
        source_record = json.loads(element)
        accession = source_record.get('accession')

        try:
            self._throttle()
            genome_id = retrieve_genome_id(accession)
            self._throttle()
            stats = retrieve_genome_stats_by_id(genome_id)
        except EnsemblApiError as exc:
            yield beam.pvalue.TaggedOutput(
                'errors',
                {
                    'genome_accession': accession,
                    'error': str(exc),
                },
            )
            return

        genome_stats = stats.get('genome_stats', {})

        record = {
            'genome_id': genome_id,
            'accession': source_record.get('accession'),
            'assembly_stats': genome_stats.get('assembly_stats'),
            'coding_stats': genome_stats.get('coding_stats'),
            'variation_stats': genome_stats.get('variation_stats'),
            'non_coding_stats': genome_stats.get('non_coding_stats'),
            'pseudogene_stats': genome_stats.get('pseudogene_stats'),
            'homology_stats': genome_stats.get('homology_stats'),
            'regulation_stats': genome_stats.get('regulation_stats'),
        }

        yield record

    def _throttle(self) -> None:
        if self.api_call_delay_seconds > 0:
            time.sleep(self.api_call_delay_seconds)
