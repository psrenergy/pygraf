# Converts a Sddp result binary file to Comma Delimited Values (CSV) file.
from __future__ import print_function
import psr.graf

import csv


def graf_to_csv(graf_file_path, csv_file_path, **csv_kwargs):
    # type: (str, str, **str) -> None
    with psr.graf.open_bin(graf_file_path, hdr_info=False) as graf_file, \
            open(csv_file_path, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file, **csv_kwargs)
        csv_writer.writerow(['stage', 'scenario', 'block'] + graf_file.agents)
        total_agents = len(graf_file.agents)
        total_stages = graf_file.stages
        total_scenarios = graf_file.scenarios
        row_values = [0.0] * (total_agents + 3)
        for stage in range(1, total_stages + 1):
            row_values[0] = stage
            total_blocks = graf_file.blocks(stage)
            for scenario in range(1, total_scenarios + 1):
                row_values[1] = scenario
                for block in range(1, total_blocks + 1):
                    row_values[2] = block
                    row_values[3:] = graf_file.read(stage, scenario, block)
                    csv_writer.writerow(row_values)


if __name__ == "__main__":
    sddp_file = r"""sample_data/gerter.hdr"""
    csv_file = r"""gerter.csv"""

    graf_to_csv(sddp_file, csv_file, delimiter=',', quotechar='"')
