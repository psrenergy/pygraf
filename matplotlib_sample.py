from __future__ import print_function
import matplotlib.pyplot as plt
import psr.graf


if __name__ == "__main__":
    """
    Instantiate a `SddpBinaryReader` object or use the context managed function
    `open_bin`.

    Usage:
    - class SddpBinaryReader.open(file_path, kwargs)
    - open_bin(file_path, kwargs)
    file_path can be a path without extension (hdr and bin have common base names),
    with .hdr or .bin extensions.
    kwargs:
     - hdr_info: boolean, prints HDR description

    Data can be read using `class SddpBinaryReader.read(stage, scenario, block)`
    or class `SddpBinaryReader.read_blocks(stage, scenario)`.
    `stage`, `scenario`, and `block` are 1-based indexes. Their range can be
    retrieved by `.stages`, `.scenarios`, and `.blocks(scenario)` respectively.
    """

    print("Example using open(...), close()")
    filename1 = "sample_data/gerter.hdr"
    graf_file = psr.graf.BinReader()
    graf_file.open(filename1, hdr_info=True)

    print("Stages:", graf_file.stages)
    print("Agents:", graf_file.agents)

    # 1-base indexing
    stage = 1
    scenario = 1
    block = 1
    print("Specific values (per agent):", graf_file.read(stage, scenario, block))
    graf_file.close()

    # Hourly demand data plot example
    with psr.graf.open_bin("sample_data/demand.hdr", hdr_info=False) as graf_file:
        print("Stages:", graf_file.stages)
        print("Scenarios:", graf_file.scenarios)
        print("Agents:", graf_file.agents)
        stage = 1
        # Number of hours vary by stage on monthly studies.
        hours = list(range(1, graf_file.blocks(stage) + 1))
        scenario = 1

        hour_data_per_agent = []
        for hour in hours:
            data = graf_file.read(stage, scenario, hour)
            hour_data_per_agent.append(data)
        # Transpose stage_data_per_agent
        agent_data_per_hour = list(map(list, zip(*hour_data_per_agent)))

        plt.figure()
        plt.title("hourly {}, stage {}, scenario {}".format(graf_file.name, stage, scenario))
        legend_entries = []
        for iagent, agent_hour_values in enumerate(agent_data_per_hour):
            agent = graf_file.agents[iagent]
            legend_entries.append(agent)
            plt.plot(hours, agent_hour_values)

        plt.legend(legend_entries)
        plt.grid(True)
        plt.ylabel(graf_file.units)
        plt.xlabel("Hours")

    with psr.graf.open_bin("sample_data/gerter.hdr", hdr_info=False) as graf_file:
        print("Stages:", graf_file.stages)
        print("Scenarios:", graf_file.scenarios)
        print("Agents:", graf_file.agents)
        stage_start = 1
        stage_end = graf_file.stages
        stages = list(range(stage_start, stage_end))
        scenario = 1
        block = 1

        stage_data_per_agent = []
        for stage in stages:
            data = graf_file.read(stage, scenario, block)
            stage_data_per_agent.append(data)
        # Transpose stage_data_per_agent
        agent_data_per_stage = list(map(list, zip(*stage_data_per_agent)))

        plt.figure()
        plt.title("{}, scenario {}, block {}".format(graf_file.name, scenario, block))
        legend_entries = []
        for iagent, agent_stage_values in enumerate(agent_data_per_stage):
            agent = graf_file.agents[iagent]
            legend_entries.append(agent)
            plt.plot(stages, agent_stage_values)

        plt.legend(legend_entries)
        plt.grid(True)
        plt.ylabel(graf_file.units)
        plt.xlabel("Stage")
        plt.xticks(stages)
        plt.show()
