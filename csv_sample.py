# Converts a Sddp result binary file to Comma Delimited Values (CSV) file.
from __future__ import print_function
import psr.graf

import argparse
import csv
import os
import sys

_IS_PY3 = sys.version_info.major == 3


def graf_to_csv(graf_file_path, csv_file_path, **csv_kwargs):
    # type: (str, str, **str) -> None
    extra_args = {'newline': ''} if _IS_PY3 else {}
    mode = 'w' if _IS_PY3 else 'wb'
    with psr.graf.open_bin(graf_file_path, hdr_info=False) as graf_file, \
            open(csv_file_path, mode, **extra_args) as csv_file:
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
    # Read file name from command line arguments
    # - or use sample data if not provided.
    parser = argparse.ArgumentParser(
        description='Converts a Sddp result binary file to Comma '
                    'Delimited Values (CSV) file.')
    parser.add_argument('sddp_file', type=str, nargs='?',
                        help='Sddp result binary file', default=None)
    parser.add_argument('csv_file', type=str, nargs='?',
                        help='Output CSV file', default=None)
    args = parser.parse_args()

    if args.sddp_file is None:
        sddp_file = r"""sample_data/gerter.hdr"""
        sample_data = True
    else:
        sddp_file = args.sddp_file
        sample_data = False

    csv_file = args.csv_file if args.csv_file is not None \
        else os.path.splitext(sddp_file)[0] + ".csv"

    if os.path.exists(sddp_file):
        graf_to_csv(sddp_file, csv_file, delimiter=',', quotechar='"')
    else:
        if not sample_data:
            raise Exception("File not found: {}".format(sddp_file))
        else:
            raise Exception("Sample data file not found: {}".format(sddp_file))
