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
    filename1 = "data/gerter.hdr"
    sddp = psr.graf.SddpBinaryReader()
    sddp.open(filename1, hdr_info=True)

    print("Stages:", sddp.stages)
    print("Agents:", sddp.agents)

    # 1-base indexing
    stage = 1
    scenario = 1
    block = 1
    print("Specific values (per agent):", sddp.read(stage, scenario, block))
    sddp.close()

    print()
    print("Example plot by scenario, context managed")
    with psr.graf.open_bin("other/dclink.hdr", hdr_info=False) as sddp:
        print("Stages:",    sddp.stages)
        print("Scenarios:", sddp.scenarios)
        print("Agents:",    sddp.agents)

        stg = 1
        scn = 2
        data = sddp.read_blocks(stg, scn)
        blocks = range(1, len(data[0]) + 1)
        plt.figure()
        plt.title("{}, stage {}, scenario {}".format(sddp.name, stg, scn))
        legend_entries = []
        for iagent, agent_block_values in enumerate(data):
            agent = sddp.agents[iagent]
            legend_entries.append(agent)
            plt.plot(blocks, agent_block_values)

        plt.legend(legend_entries)
        plt.ylabel(sddp.units)
        plt.xticks(range(1, len(blocks) + 1, 1))
        plt.xlabel("Block")

    with psr.graf.open_bin("other/flw_pdik.hdr", hdr_info=False) as sddp:
        print("Stages:", sddp.stages)
        print("Scenarios:", sddp.scenarios)
        print("Agents:", sddp.agents)
        stg = 1
        scn = 2
        data = sddp.read_blocks(stg, scn)
        blocks = range(1, len(data[0]) + 1)
        plt.figure()
        plt.title("{}, stage {}, scenario {}".format(sddp.name, stg, scn))
        legend_entries = []
        for iagent, agent_block_values in enumerate(data):
            agent = sddp.agents[iagent]
            legend_entries.append(agent)
            plt.plot(blocks, agent_block_values)

        plt.legend(legend_entries)
        plt.ylabel(sddp.units)
        plt.xlabel("Block")
        plt.xticks(range(1, len(blocks) + 1, 1))
        plt.show()
