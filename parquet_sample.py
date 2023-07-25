# Converts a Sddp result binary file to Apache Parquet file format.
from __future__ import print_function
import psr.graf

import pyarrow as pa
import pyarrow.parquet as pq


# Change this number to optimize the number of stages read/written
# in parquet files.
_stage_chunk_size = 10


def chunkfy(data, chunk_size):
    # type: (list, int) -> list
    n = len(data)
    l = data
    k = chunk_size
    return [l[i * (n // k) + min(i, n % k):
              (i + 1) * (n // k) + min(i + 1, n % k)] for i in range(k)]


def sddp_to_parquet(sddp_file_path, parquet_file_path):
    # type: (str, str) -> None
    with psr.graf.open_bin(sddp_file_path, hdr_info=False) as sddp:
        # The code below specifies the table layout.
        fields = [
            pa.field('stage', pa.int64()),
            pa.field('scenario', pa.int64()),
            pa.field('block', pa.int64())
        ]
        fields.extend([pa.field(agent, pa.float32()) for agent in sddp.agents])

        first_chunk = True
        parquet_writer = None
        for i_chunk, stage_chunk in enumerate(
                chunkfy(list(range(1, sddp.stages + 1)), _stage_chunk_size)):
            stages = []  # Stage number column.
            scenarios = []  # Scenario number column.
            blocks = []  # Blocks number column.
            agents = []  # Stores all agents columns data.
            for _ in sddp.agents:
                agents.append([])

            for stage in stage_chunk:
                for scenario in range(1, sddp.scenarios + 1):
                    data = sddp.read_blocks(sddp.stages, sddp.scenarios)
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


def parquet_to_csv(parquet_file_path, csv_file_path):
    # type: (str, str) -> None
    df1 = pq.read_table(parquet_file_path).to_pandas()

    df1.to_csv(
        csv_file_path,
        sep=',',
        index=False,
        mode='w',
        encoding='utf-8')


if __name__ == "__main__":
    sddp_file = r"""other/dclink.hdr"""
    parquet_file = r"""dclink.parquet"""
    sddp_to_parquet(sddp_file, parquet_file)

    # Convert parquet to CSV for verification.
    # This might take a while for big hdr/bin files.
    parquet_to_csv(parquet_file, "sample.csv")

