from __future__ import print_function
from contextlib import contextmanager
import os
import struct
import sys

_IS_PY2 = sys.version_info.major == 2

if not _IS_PY2:
    from typing import Union

_VERSION = "1.1.0"

# Number of bytes in a word (int32, float, ...)
_WORD = 4

# Check whether pandas' dataframe is available.
_HAS_PANDAS = False
try:
    import pandas as pd

    _HAS_PANDAS = True
except ImportError:
    pd = None
    _HAS_PANDAS = False


def version():
    # type: () -> str
    return _VERSION


class BinReader:
    """
    SDDP binary data reader class.
    """
    BLOCK_TYPE_BLOCK = 0
    BLOCK_TYPE_HOUR = 1
    STAGE_TYPE_WEEKLY = 1
    STAGE_TYPE_MONTHLY = 2
    STAGE_TYPE_13MONTHLY = 6
    STAGE_DESCRIPTION = {STAGE_TYPE_WEEKLY: "weekly",
                         STAGE_TYPE_MONTHLY: "monthly",
                         STAGE_TYPE_13MONTHLY: "13 months"}
    BLOCK_DESCRIPTION = {BLOCK_TYPE_BLOCK: "block",
                         BLOCK_TYPE_HOUR: "hour"}

    def __init__(self):
        # string encoding
        self._encoding = "utf-8"
        # file references
        self.__hdr_file_path = ""
        self.__bin_file_path = ""
        self.__bin_file_handler = None
        self.__single_bin_mode = False
        self.__bin_offset = 0
        self.name = ""
        # print hdr information
        self.__print_metadata = False
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
        # type: (str, dict) -> None
        """
        Opens a single binary or HDR and BIN file pairs for reading,
        when one of then is specified. If a file path without extension is
        specified, tries to open a pair of .hdr and .bin file.
        The BIN file stays open during BinReader lifetime.

        Keyword arguments:
        print_metadata -- Print files metadata to stdout (Default = False).
        encoding -- encoding to decode strings in binary files
                    (Default = utf-8).

        Raises FileNotFoundError if one of the required files is not found.

        Non thread-safe method.
        """
        # file paths
        base_path, ext = os.path.splitext(file_path)
        if ext.lower() == ".hdr" or ext.lower() == ".bin" or ext == "":
            self.__hdr_file_path = base_path + ".hdr"
            self.__bin_file_path = base_path + ".bin"
        else:
            self.__hdr_file_path = file_path
            self.__single_bin_mode = True
        self.name = os.path.basename(base_path)

        self._encoding = kwargs.get('encoding', 'utf-8')

        # check file existence
        if not os.path.exists(self.__hdr_file_path):
            if not self.__single_bin_mode:
                error_msg = "HDR file not found: {}".format(self.__hdr_file_path)
            else:
                error_msg = "File not found: {}".format(self.__hdr_file_path)
            if _IS_PY2:
                FileNotFoundError = IOError
            raise FileNotFoundError(error_msg)

        if not self.__single_bin_mode:
            if not os.path.exists(self.__bin_file_path):
                error_msg = "BIN file not found: {}".format(self.__bin_file_path)
                if _IS_PY2:
                    FileNotFoundError = IOError
                raise FileNotFoundError(error_msg)

        # additional parameters
        for key, value in kwargs.items():
            if key == "print_metadata" and value:
                self.__print_metadata = True

        if not self.__single_bin_mode:
            # read HDR
            with open(self.__hdr_file_path, 'rb') as hdr_file:
                self.__read_hdr(hdr_file)

            # read BIN and keep it open
            self.__bin_file_handler = open(self.__bin_file_path, 'rb')
        else:
            # Read single binary file and keep it open.
            data_file = open(self.__hdr_file_path, 'rb')
            self.__read_hdr(data_file)
            self.__bin_offset = data_file.tell()
            self.__bin_file_handler = data_file

    def close(self):
        # type: () -> None
        """Closes the binary file for reading."""
        if not self.__bin_file_handler.closed:
            self.__bin_file_handler.close()

    def __read_hdr(self, input_stream):
        # type: (any) -> None
        def unpack_int():
            # type: () -> int
            """Unpack 4 bytes as integer from input_stream and move its
            position by 4 bytes.
            """
            return struct.unpack('i', input_stream.read(_WORD))[0]

        def unpack_str(length):
            # type: (int) -> str
            # Unpack variable length string.
            bytes_value = struct.unpack(str(length) + 's',
                                        input_stream.read(length))[0]
            return bytes_value.decode(self._encoding).strip()

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

        if self.__print_metadata:
            if not self.__single_bin_mode:
                print("HDR/BIN graf metadata:")
            else:
                print("graf metadata:")
            print("  Binary file version:", self.bin_version)
            print("  First stage:", self.initial_stage)
            print("  Number of stages:", self.stages)
            print("  Number of scenarios:", self.scenarios)
            print("  Number of agents:", agents_count)
            print("  Varies per scenario:", self.varies_by_scenario)
            print("  Varies per block/hour:", self.varies_by_block)
            print("  Time mapping: {}".format(
                BinReader.BLOCK_DESCRIPTION[self.hour_or_block]))
            print("  Type of stage: {}".format(
                BinReader.STAGE_DESCRIPTION[self.stage_type]))
            print("  Initial month/week:", self.initial_month)
            print("  Initial year:", self.initial_year)
            print("  Units:", self.units)
            print("  Stored name's length:", self.name_length)

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

    def __check_indexes(self, stage_to_check, scenario_to_check,
                        block_to_check=0):
        # type: (int, int, int) -> None
        stage_msg = "Stage {} out of range ({})."
        scenario_msg = "Scenario {} out of range ({})."
        block_msg = "Block {} out of range ({} for stage {})."
        if stage_to_check > self.stages:
            raise IndexError(stage_msg.format(stage_to_check, self.stages))

        if scenario_to_check > self.scenarios:
            raise IndexError(
                scenario_msg.format(scenario_to_check, self.scenarios))

        total_blocks = self.blocks(stage_to_check)
        if block_to_check > 0 and block_to_check > total_blocks:
            raise IndexError(block_msg.format(block_to_check, total_blocks,
                                              stage_to_check))

    def __seek(self, i_stage, i_scenario, i_block):
        # type: (int, int, int) -> None
        # i_scenario, i_block are 1-based indexes; i_stage is 0-based.
        index = (self.bin_offsets[i_stage] * self.scenarios
                 + self.blocks(i_stage + 1) * (i_scenario - 1)
                 + (i_block - 1)) * len(self.agents)

        offset_from_start = self.__bin_offset + index * _WORD
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
        all_values = struct.unpack(fmt,
                                   self.__bin_file_handler.read(_WORD * count))
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
    # type: (str, dict) -> None
    """
    Open SDDP binary files (.hdr and .bin) for reading provided a file path
    for one of them or an file path without extension. Yields a
    SddpBinaryReader class.

    Keyword arguments:
        print_metadata -- Print files metadata to stdout (Default = False).
        encoding -- encoding to decode strings in binary files
                    (Default = utf-8).
    """
    obj = BinReader()
    obj.open(file_path, **kwargs)
    yield obj
    obj.close()


def load_as_dataframe(file_path, **kwargs):
    # type: (str, dict) -> Union[pd.DataFrame, None]
    if _HAS_PANDAS:
        with open_bin(file_path, print_metadata=False, **kwargs) as graf_file:
            total_agents = len(graf_file.agents)
            row_values = [0.0] * (total_agents)
            data = []
            index_values = []
            for stage in range(1, graf_file.stages + 1):
                total_blocks = graf_file.blocks(stage)
                for scenario in range(1, graf_file.scenarios + 1):
                    for block in range(1, total_blocks + 1):
                        data.append(graf_file.read(stage, scenario, block))
                        index_values.append((stage, scenario, block))
            index = pd.MultiIndex.from_tuples(index_values,
                                              names=['stage', 'scenario',
                                                     'block'])
            return pd.DataFrame(data, index=index, columns=graf_file.agents)
    else:
        return None
