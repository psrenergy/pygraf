# Converts a SDDP result binary file to Apache Parquet file format.
from __future__ import print_function
import psr.graf

import argparse
import os

import pyarrow as pa
import pyarrow.parquet as pq


# Change this number to optimize the number of stages read/written
# in parquet files.
_stage_chunk_size = 10


def chunkfy(data: list, chunk_size: int) -> list:
    n = len(data)
    l = data
    k = chunk_size
    return [l[i * (n // k) + min(i, n % k):
              (i + 1) * (n // k) + min(i + 1, n % k)] for i in range(k)]


def graf_to_parquet(graf_file_path: str, parquet_file_path: str):
    with psr.graf.open_bin(graf_file_path) as graf_file:
        # The code below specifies the table layout.
        fields = [
            pa.field('stage', pa.int64()),
            pa.field('scenario', pa.int64()),
            pa.field('block', pa.int64())
        ]
        fields.extend([pa.field(agent, pa.float32())
                       for agent in graf_file.agents])

        first_chunk = True
        parquet_writer = None
        for i_chunk, stage_chunk in enumerate(
                chunkfy(list(range(graf_file.min_stage,
                                   graf_file.max_stage + 1)),
                        _stage_chunk_size)):
            stages = []  # Stage number column.
            scenarios = []  # Scenario number column.
            blocks = []  # Blocks number column.
            agents = []  # Stores all agents columns data.
            for _ in graf_file.agents:
                agents.append([])

            for stage in stage_chunk:
                for scenario in range(1, graf_file.scenarios + 1):
                    data = graf_file.read_blocks(stage, scenario)
                    total_blocks = len(data[0])
                    current_blocks = list(range(1, total_blocks + 1))

                    stages.extend([stage] * total_blocks)
                    scenarios.extend([scenario] * total_blocks)
                    blocks.extend(current_blocks)
                    [agents[i_agent].extend(block_data)
                     for i_agent, block_data in enumerate(data)]

            arrays = [
                pa.array(stages),
                pa.array(scenarios),
                pa.array(blocks)
            ]
            arrays.extend([pa.array(agent) for agent in agents])
            parquet_table = pa.Table.from_arrays(arrays=arrays,
                                                 schema=pa.schema(fields))
            if first_chunk:
                parquet_writer = pq.ParquetWriter(parquet_file_path,
                                                  parquet_table.schema)
                first_chunk = False
            parquet_writer.write_table(parquet_table)

        # Close the parquet writer.
        parquet_writer.close()


def parquet_to_csv(parquet_file_path: str, csv_file_path: str):
    df1 = pq.read_table(parquet_file_path).to_pandas()

    df1.to_csv(
        csv_file_path,
        sep=',',
        index=False,
        mode='w',
        encoding='utf-8')


if __name__ == "__main__":
    # Read file name from command line arguments
    # - or use sample data if not provided.
    parser = argparse.ArgumentParser(
        description='Converts a SDDP result binary file to Apache Parquet '
                    'file format.')
    parser.add_argument('sddp_file', type=str, nargs='?',
                        help='SDDP result binary file', default=None)
    parser.add_argument('parquet_file', type=str, nargs='?',
                        help='Output Parquet file', default=None)
    args = parser.parse_args()

    if args.sddp_file is None:
        sddp_file = r"""sample_data/demand.hdr"""
        sample_data = True
    else:
        sddp_file = args.sddp_file
        sample_data = False

    parquet_file = args.parquet_file if args.parquet_file is not None \
        else os.path.splitext(sddp_file)[0] + ".parquet"

    if os.path.exists(sddp_file):
        graf_to_parquet(sddp_file, parquet_file)
    else:
        if not sample_data:
            raise Exception("File not found: {}".format(sddp_file))
        else:
            raise Exception("Sample data file not found: {}".format(sddp_file))
