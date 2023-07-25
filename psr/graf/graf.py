#!/usr/bin/python
from __future__ import print_function
from contextlib import contextmanager
import os
import struct
import sys
from typing import Union

# Number of bytes in a word (int32, float, ...)
_WORD = 4

_IS_PY2 = sys.version_info.major == 3


# Check whether pandas' dataframe is available.
_HAS_PANDAS = False
try:
    import pandas as pd
    _HAS_PANDAS = True
except ImportError:
    pd = None
    _HAS_PANDAS = False


class BinReader:
    """
    SDDP binary data reader class.
    """
    def __init__(self):
        # block type
        self.BLOCK = 0
        self.HOUR = 1
        # stage type
        self.MONTHLY = 2
        self.WEEKLY = 1
        # file references
        self.__hdr_file_path = ""
        self.__bin_file_path = ""
        self.__bin_file_handler = None
        self.name = ""
        # print hdr information
        self.__hdr_info = False
        # hdr file data
        self.bin_version = None
        self.initial_stage = None
        self.stages = None
        self.scenarios = None
        self.varies_by_scenario = None
        self.varies_by_block = None
        self.stage_type = None
        self.initial_month = None
        self.initial_year = None
        self.units = None
        self.name_length = None
        self.bin_offsets = None
        self.agents = None

    def __del__(self):
        if self.__bin_file_handler is not None:
            self.close()

    def open(self, file_path, **kwargs):
        # type: (str, **bool) -> None
        """
        Open a HDR and a BIN file for reading, given one of them or an 
        extensionless file path. The BIN file stays open.

        Keyword arguments:
        hdr_info -- Print files metadata to stdout (Default = False).

        Raises FileNotFoundError if one of the required files is not found.

        Non thread-safe method.
        """
        # file paths
        base_path = self.__remove_file_extension(file_path)
        self.__hdr_file_path = base_path + ".hdr"
        self.__bin_file_path = base_path + ".bin"
        self.name = os.path.basename(base_path)

        # check file existence
        if not os.path.exists(self.__hdr_file_path):
            error_msg = "HDR file not found: {}".format(self.__hdr_file_path)
            if _IS_PY2:
                FileNotFoundError = IOError
            raise FileNotFoundError(error_msg)

        if not os.path.exists(self.__bin_file_path):
            error_msg = "BIN file not found: {}".format(self.__bin_file_path)
            if _IS_PY2:
                FileNotFoundError = IOError
            raise FileNotFoundError(error_msg)

        # additional parameters
        for key, value in kwargs.items():
            if key == "hdr_info" and value:
                self.__hdr_info = True

        # read HDR
        with open(self.__hdr_file_path, 'rb') as file:
            self.__read_hdr(file)

        # read BIN and keep it open
        self.__bin_file_handler = open(self.__bin_file_path, 'rb')

    def close(self):
        # type: () -> None
        """Closes the binary file for reading."""
        if not self.__bin_file_handler.closed:
            self.__bin_file_handler.close()

    @staticmethod
    def __remove_file_extension(file_path):
        # type: (str) -> str
        return os.path.splitext(file_path)[0]

    def __read_hdr(self, input_stream):
        # type: (any) -> None
        def unpack_int():
            # type: () -> int
            """Unpack 4 bytes as integer from input_stream and move its position by.
            """
            return struct.unpack('i', input_stream.read(_WORD))[0]

        # Unpack variable length string
        def unpack_str(length):
            # type: (int) -> str
            bytes_value = struct.unpack(str(length) + 's',
                                        input_stream.read(length))[0]
            return bytes_value.decode().strip()

        # Record #1
        seek_curpos = 1  # Seek from current position flag
        input_stream.seek(_WORD, seek_curpos)
        self.bin_version = unpack_int()
        input_stream.seek(_WORD, seek_curpos)

        # Record #2
        input_stream.seek(_WORD, seek_curpos)
        self.initial_stage = unpack_int()
        self.stages = unpack_int()
        self.scenarios = unpack_int()
        agents_count = unpack_int()
        self.varies_by_scenario = unpack_int()
        self.varies_by_block = unpack_int()
        self.hour_or_block = unpack_int()
        self.stage_type = unpack_int()
        self.initial_month = unpack_int()
        self.initial_year = unpack_int()

        self.units = unpack_str(7)

        self.name_length = unpack_int()

        if self.__hdr_info:
            print("HDR data:")
            print("  Binary file version:", self.bin_version)
            print("  First stage:", self.initial_stage)
            print("  Number of stages:", self.stages)
            print("  Number of scenarios:", self.scenarios)
            print("  Number of agents:", agents_count)
            print("  Varies per scenario:", self.varies_by_scenario)
            print("  Varies per block/hour:", self.varies_by_block)
            if self.hour_or_block == self.BLOCK:
                print("  Type of data: Block")
            else:
                print("  Type of data: Hour")
            if self.stage_type == self.MONTHLY:
                print("  Type of stage: Monthly")
            else:
                print("  Type of stage: Weekly")
            print("  Initial month/week:", self.initial_month)
            print("  Initial year:", self.initial_year)
            print("  Units:", self.units)
            print("  Stored name's lenght:", self.name_length)

        input_stream.seek(_WORD, seek_curpos)

        # Record #3
        input_stream.seek(_WORD, seek_curpos)

        self.bin_offsets = [0] * (self.stages + 1)
        for i_stage in range(self.stages + 1):
            self.bin_offsets[i_stage] = unpack_int()

        input_stream.seek(_WORD, seek_curpos)

        # Agent names
        self.agents = []
        for i_agent in range(agents_count):
            string_length = unpack_int()
            self.agents.append(unpack_str(string_length))
            # discard unused bytes
            input_stream.read(_WORD)

    def __check_indexes(self, stage_to_check, scenario_to_check, block_to_check=0):
        # type: (int, int, int) -> None
        stage_msg = "Stage {} out of range ({})."
        scenario_msg = "Scenario {} out of range ({})."
        block_msg = "Block {} out of range ({} for stage {})."
        if stage_to_check > self.stages:
            raise IndexError(stage_msg.format(stage_to_check, self.stages))

        if scenario_to_check > self.scenarios:
            raise IndexError(scenario_msg.format(scenario_to_check, self.scenarios))

        total_blocks = self.blocks(stage_to_check)
        if block_to_check > 0 and block_to_check > total_blocks:
            raise IndexError(block_msg.format(block_to_check, total_blocks,
                                              stage_to_check))

    def __seek(self, i_stage, i_scenario, i_block):
        # type: (int, int, int) -> None
        # i_scenario, i_block are 1-based indexes; i_stage is 0-based.
        index = (self.bin_offsets[i_stage] * self.scenarios \
                + self.blocks(i_stage + 1) * (i_scenario - 1) \
                + (i_block - 1)) * len(self.agents)

        offset_from_start = index * _WORD
        seek_from_start = 0
        self.__bin_file_handler.seek(offset_from_start, seek_from_start)

    def blocks(self, stage):
        # type: (int) -> int
        """Number of blocks for a given stage. 1-based stage."""
        return self.bin_offsets[stage] - self.bin_offsets[stage - 1]

    def read(self, stage, scenario, block):
        # type: (int, int, int) -> tuple
        """
        Read data of a given stage, scenario, and block. Returns a list with
        values for each agent. 

        Raises IndexError if stage, scenario, or block is out of bounds.

        Non thread-safe.
        """
        self.__check_indexes(stage, scenario, block)
        istage = stage - 1
        self.__seek(istage, scenario, block)
        agents = len(self.agents)
        fmt = str(agents) + 'f'
        return struct.unpack(fmt, self.__bin_file_handler.read(agents * _WORD))

    def read_blocks(self, stage, scenario):
        # type: (int, int) -> list
        """
        Read data of a given stage and scenario. Returns a list
        containing lists with block data, for each agent.

        Raises IndexError if stage or scenario is out of bounds.
        
        Non thread-safe.
        """
        self.__check_indexes(stage, scenario)
        i_stage = stage - 1
        self.__seek(i_stage, scenario, 1)

        agents = len(self.agents)
        blocks = self.bin_offsets[i_stage + 1] - self.bin_offsets[i_stage]
        count = blocks * agents

        fmt = "{}f".format(count)
        all_values = struct.unpack(fmt, self.__bin_file_handler.read(_WORD * count))
        len_per_agent = int(len(all_values) / agents)

        lists = []
        for i_agent in range(agents):
            values = [0.0] * len_per_agent
            for i_value in range(len_per_agent):
                values[i_value] = all_values[i_agent + i_value * agents]
            lists.append(values)
        return lists


@contextmanager
def open_bin(file_path, **kwargs):
    # type: (str, **bool) -> None
    """
    Open SDDP binary files (.hdr and .bin) for reading provided a file path
    for one of them or an extensionless file path. Yields a SddpBinaryReaader
    class.

    Keyword arguments:
        hdr_info -- Print files metadata to stdout (Default = False).
    """
    obj = BinReader()
    obj.open(file_path, **kwargs)
    yield obj
    obj.close()


def load_as_dataframe(file_path):
    # type: (str) -> Union[pd.DataFrame, None]
    if _HAS_PANDAS:
        with open_bin(file_path, hdr_info=False) as graf_file:
            total_agents = len(graf_file.agents)
            row_values = [0.0] * (total_agents + 3)
            data = []
            for stage in range(1, graf_file.stages + 1):
                row_values[0] = stage
                total_blocks = graf_file.blocks(stage)
                for scenario in range(1, graf_file.scenarios + 1):
                    row_values[1] = scenario
                    for block in range(1, total_blocks + 1):
                        row_values[2] = block
                        row_values[3:] = graf_file.read(stage, scenario, block)
                        data.append(row_values[:])
            return pd.DataFrame(data, columns=['stage', 'scenario', 'block']
                                              + graf_file.agents)
    else:
        return None
