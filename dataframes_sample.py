# Converts a Sddp result binary file into pandas.DataFrame.
from __future__ import print_function
import psr.graf
import pandas


def graf_to_dataframe(graf_file_path):
    # type: (str) -> pandas.DataFrame
    with psr.graf.open_bin(graf_file_path) as graf_file:
        total_agents = len(graf_file.agents)
        total_stages = graf_file.stages
        total_scenarios = graf_file.scenarios
        row_values = [0.0] * (total_agents + 3)
        data = []
        for stage in range(graf_file.min_stage, graf_file.max_stage + 1):
            row_values[0] = stage
            total_blocks = graf_file.blocks(stage)
            for scenario in range(1, total_scenarios + 1):
                row_values[1] = scenario
                for block in range(1, total_blocks + 1):
                    row_values[2] = block
                    row_values[3:] = graf_file.read(stage, scenario, block)
                    data.append(row_values[:])
        hour_or_block = 'hour' if graf_file.hour_or_block else 'block'
        columns = ('stage', 'scenario', hour_or_block) + graf_file.agents
        return pandas.DataFrame(data, columns=columns)


if __name__ == "__main__":
    sddp_file = r"""sample_data/gerter.hdr"""
    csv_file = r"""gerter.csv"""

    df = graf_to_dataframe(sddp_file)
    print(df.head())
    df.to_csv(csv_file, index=False)

    # Or alternatively:
    df2 = psr.graf.load_as_dataframe(sddp_file)
    print(df2.head())
