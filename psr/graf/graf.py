import csv
from contextlib import contextmanager
from itertools import islice
import pathlib
import os
import struct
from typing import Tuple, Optional, Union


__version__ = "2.1.3"

# Number of bytes in a word (int32, float, ...)
_WORD = 4

# Check whether pandas' dataframe is available.
_HAS_PANDAS = False
try:
    import pandas as pd
    import numpy as np

    _HAS_PANDAS = True
except ImportError:
    pd = None
    _HAS_PANDAS = False


def version() -> str:
    return __version__


class GrafError(Exception):
    pass


class GrafIOError(GrafError):
    pass


class _GrafReaderBase:
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
        self._name = ""
        # hdr file data
        self._case_initial_stage = None
        self._stages = None
        self._min_stage = None
        self._max_stage = None
        self._scenarios = None
        self._varies_by_scenario = None
        self._varies_by_block = None
        self._hour_or_block = None
        self._stage_type = None
        self._initial_year = None
        self._units = None
        self._agents = None

    @property
    def initial_stage(self) -> int:
        return self._case_initial_stage

    @property
    def stages(self) -> int:
        return self._stages

    @property
    def min_stage(self) -> int:
        return self._min_stage

    @property
    def max_stage(self) -> int:
        return self._max_stage

    @property
    def scenarios(self) -> int:
        return self._scenarios

    @property
    def varies_by_scenario(self) -> int:
        return self._varies_by_scenario

    @property
    def varies_by_block(self) -> int:
        return self._varies_by_block

    @property
    def hour_or_block(self) -> int:
        return self._hour_or_block

    @property
    def stage_type(self) -> int:
        return self._stage_type

    @property
    def initial_year(self) -> int:
        return self._initial_year

    @property
    def units(self) -> str:
        return self._units

    @property
    def agents(self) -> tuple:
        return self._agents

    @property
    def name(self) -> str:
        return self._name

    def open(self, file_path: Union[str, pathlib.Path], **kwargs):
        pass

    def close(self):
        pass

    def _check_indexes(self, stage_to_check: int, scenario_to_check: int,
                       block_to_check:int = 0):
        if (stage_to_check > self._max_stage or
                stage_to_check < self._min_stage):
            raise IndexError(f"Stage {stage_to_check} out of range "
                             f"({self._min_stage}, {self._max_stage}).")

        if scenario_to_check > self._scenarios:
            raise IndexError(f"Scenario {scenario_to_check} "
                             f"out of range ({self._scenarios}).")

        total_blocks = self.blocks(stage_to_check)
        if block_to_check > 0 and block_to_check > total_blocks:
            raise IndexError(f"Block {block_to_check} out of range "
                             f"({total_blocks} for stage {stage_to_check}).")

    def blocks(self, stage: int) -> int:
        pass


class BaseBinReader(_GrafReaderBase):
    """
    SDDP binary data reader class.
    """

    def __init__(self):
        super(BaseBinReader, self).__init__()
        # file references
        self.__hdr_file_path = ""
        self.__bin_file_path = ""
        self._bin_file_handler = None
        self.__single_bin_mode = False
        self.__bin_offset = 0
        # print hdr information
        self._print_metadata = False
        # hdr file data
        self._bin_version = None
        self._name_length = None
        self._bin_offsets = None

    def __del__(self):
        if self._bin_file_handler is not None:
            self.close()

    @property
    def bin_version(self) -> int:
        return self._bin_version

    @property
    def name_length(self) -> int:
        return self._name_length

    def open(self, file_path: Union[str, pathlib.Path], **kwargs):
        """
        Opens a single binary or HDR and BIN file pairs for reading,
        when one of then is specified. If a file path without extension is
        specified, tries to open a pair of .hdr and .bin file.
        The BIN file stays open during BinReader lifetime.

        Keyword arguments:
        print_metadata -- Print files metadata to stdout (Default = False).
        encoding -- encoding to decode strings in binary files
                    (Default = utf-8).

        Non thread-safe method.
        """
        self._encoding = kwargs.get('encoding', 'utf-8')
        self._print_metadata = kwargs.get('print_metadata', False)

        # file paths
        file_path = str(file_path)
        base_path, ext = os.path.splitext(file_path)
        if ext.lower() == ".hdr" or ext.lower() == ".bin" or ext == "":
            self.__hdr_file_path = base_path + ".hdr"
            self.__bin_file_path = base_path + ".bin"
        else:
            self.__hdr_file_path = file_path
            self.__single_bin_mode = True
        self._name = os.path.basename(base_path)

        # Check files existence.
        if not os.path.exists(self.__hdr_file_path):
            if not self.__single_bin_mode:
                error_msg = f"HDR file not found: {self.__hdr_file_path}"
            else:
                error_msg = f"File not found: {self.__hdr_file_path}"
            raise FileNotFoundError(error_msg)

        if not self.__single_bin_mode:
            if not os.path.exists(self.__bin_file_path):
                error_msg = f"BIN file not found: {self.__bin_file_path}"
                raise FileNotFoundError(error_msg)

        if not self.__single_bin_mode:
            # read HDR
            with open(self.__hdr_file_path, 'rb') as hdr_file:
                self.__read_hdr(hdr_file)

            # read BIN and keep it open
            self._bin_file_handler = open(self.__bin_file_path, 'rb')
        else:
            # Read single binary file and keep it open.
            data_file = open(self.__hdr_file_path, 'rb')
            self.__read_hdr(data_file)
            self.__bin_offset = data_file.tell()
            self._bin_file_handler = data_file

    def close(self):
        """Closes the binary file for reading."""
        if not self._bin_file_handler.closed:
            self._bin_file_handler.close()

    def __read_hdr(self, input_stream: any):
        def unpack_int() -> int:
            """Unpack 4 bytes as integer from input_stream and move its
            position by 4 bytes.
            """
            return struct.unpack('i', input_stream.read(_WORD))[0]

        def unpack_str(length) -> str:
            # Unpack variable length string.
            bytes_value = struct.unpack(str(length) + 's',
                                        input_stream.read(length))[0]
            return bytes_value.decode(self._encoding).strip()

        # Record #1
        seek_curpos = 1  # Seek from current position flag
        input_stream.seek(_WORD, seek_curpos)
        self._bin_version = unpack_int()
        input_stream.seek(_WORD, seek_curpos)

        # Record #2
        input_stream.seek(_WORD, seek_curpos)
        self._min_stage = unpack_int()
        self._max_stage = unpack_int()
        self._scenarios = unpack_int()
        agents_count = unpack_int()
        self._varies_by_scenario = unpack_int()
        self._varies_by_block = unpack_int()
        self._hour_or_block = unpack_int()
        self._stage_type = unpack_int()
        self._case_initial_stage = unpack_int()
        self._initial_year = unpack_int()
        self._units = unpack_str(7)
        self._name_length = unpack_int()
        self._stages = self._max_stage - self._min_stage + 1

        if self._varies_by_scenario == 0:
            self._scenarios = 1

        if self._print_metadata:
            if not self.__single_bin_mode:
                print("HDR/BIN graf metadata:")
            else:
                print("graf metadata:")
            print("  Binary file version:", self._bin_version)
            print("  First stage:", self._min_stage)
            print("  Number of stages:", self._stages)
            print("  Number of scenarios:", self._scenarios)
            print("  Number of agents:", agents_count)
            print("  Varies per scenario:", self._varies_by_scenario)
            print("  Varies per block/hour:", self._varies_by_block)
            print("  Time mapping:",
                  BinReader.BLOCK_DESCRIPTION[self._hour_or_block])
            print("  Type of stage:",
                  BinReader.STAGE_DESCRIPTION[self._stage_type])
            print("  Case Initial month/week:", self._case_initial_stage)
            print("  Case Initial year:", self._initial_year)
            print("  Units:", self._units)
            print("  Stored name's length:", self._name_length)

        input_stream.seek(_WORD, seek_curpos)

        # Record #3
        input_stream.seek(_WORD, seek_curpos)

        self._bin_offsets = [0] * (self._stages + 1)
        for i_stage in range(self._stages + 1):
            self._bin_offsets[i_stage] = unpack_int()

        input_stream.seek(_WORD, seek_curpos)

        # Agent names
        _agents = []
        for i_agent in range(agents_count):
            string_length = unpack_int()
            _agents.append(unpack_str(string_length))
            # discard unused bytes
            input_stream.read(_WORD)
        self._agents = tuple(_agents)

    def _seek(self, i_stage: int, i_scenario: int, i_block: int):
        # i_scenario, i_block are 1-based indexes; i_stage is 0-based.
        index = (self._bin_offsets[i_stage] * self._scenarios
                 + self.blocks(i_stage + 1) * (i_scenario - 1)
                 + (i_block - 1)) * len(self._agents)

        offset_from_start = self.__bin_offset + index * _WORD
        seek_from_start = 0
        self._bin_file_handler.seek(offset_from_start, seek_from_start)

    def blocks(self, stage: int) -> int:
        """Number of blocks for a given stage. 1-based stage."""
        istage = stage - self._min_stage + 1
        if self._varies_by_block != 0:
            return self._bin_offsets[istage] - self._bin_offsets[istage - 1]
        return 1

    def read(self, stage: int, scenario: int, block: int) -> tuple:
        """
        Read data of a given stage, scenario, and block. Returns a list with
        values for each agent. 

        Raises IndexError if stage, scenario, or block is out of bounds.

        Non thread-safe.
        """
        self._check_indexes(stage, scenario, block)
        istage = stage - self._min_stage
        self._seek(istage, scenario, block)
        agents = len(self._agents)
        fmt = str(agents) + 'f'
        return struct.unpack(fmt, self._bin_file_handler.read(agents * _WORD))

    def read_blocks(self, stage: int, scenario: int) -> list:
        """
        Read data of a given stage and scenario. Returns a list
        containing lists with block data, for each agent.

        Raises IndexError if stage or scenario is out of bounds.
        
        Non thread-safe.
        """
        self._check_indexes(stage, scenario)
        i_stage = stage - 1
        self._seek(i_stage, scenario, 1)

        agents = len(self._agents)
        blocks = self._bin_offsets[i_stage + 1] - self._bin_offsets[i_stage]
        count = blocks * agents

        all_values = struct.unpack(f"{count}f",
                                   self._bin_file_handler.read(_WORD * count))
        len_per_agent = int(len(all_values) / agents)

        lists = []
        for i_agent in range(agents):
            values = [0.0] * len_per_agent
            for i_value in range(len_per_agent):
                values[i_value] = all_values[i_agent + i_value * agents]
            lists.append(values)
        return lists

    def read_blocks_as_array(self, stage: int, scenario: int) -> (
            Tuple)[Tuple[float, ...], ...]:
        self._check_indexes(stage, scenario)
        i_stage = stage - 1
        self._seek(i_stage, scenario, 1)

        agents = len(self._agents)
        blocks = self._bin_offsets[i_stage + 1] - self._bin_offsets[
            i_stage]
        count = blocks * agents

        all_values = struct.unpack(f"{count}f",
            self._bin_file_handler.read(_WORD * count))
        len_per_agent = int(len(all_values) / agents)

        iterator = iter(all_values)
        return tuple(
            tuple(islice(iterator, len_per_agent)) for _ in range(agents))


if not _HAS_PANDAS:
    class BinReader(BaseBinReader):
        pass

else:
    class BinReader(BaseBinReader):
        """
        A modified version of the psr.graf.BinReader class with an alternative
        to the read_blocks method, that avoids some expensive for loops by
        using numpy to reshape the incoming stream into a 2D array.
        """

        def read_blocks_as_array(self, stage: int,
                                 scenario: int) -> np.ndarray:
            """
            Read data of a given stage and scenario. Returns a 2D numpy array
            with dimensions (blocks, agents).

            Raises IndexError if stage or scenario is out of bounds.

            Author: corypdavis
            """
            self._check_indexes(stage, scenario)
            i_stage = stage - 1
            self._seek(i_stage, scenario, 1)

            agents = len(self._agents)
            blocks = self._bin_offsets[i_stage + 1] - self._bin_offsets[
                i_stage]
            count = blocks * agents

            all_values = struct.unpack(f"{count}f",
                self._bin_file_handler.read(_WORD * count))
            # I cant see why this is not just blocks
            len_per_agent = int(len(all_values) / agents)

            return np.array(all_values, dtype=np.float32).reshape(
                (len_per_agent, agents))


class CsvReader(_GrafReaderBase):
    def __init__(self):
        super(CsvReader, self).__init__()
        self.__csv_file_path = ""
        self.__data = {}
        self.__max_blocks_per_stage = {}

    def open(self, file_path: Union[str, pathlib.Path], **kwargs):
        """
        Opens a single csv file for reading.
        """
        self._encoding = kwargs.get('encoding', 'utf-8')
        file_path = str(file_path)
        self.__csv_file_path = file_path
        self._name = os.path.basename(self.__csv_file_path)

        # Check file existence.
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"CSV file not found: {file_path}")

        with open(self.__csv_file_path, 'r',
                  encoding=self._encoding) as csv_file:
            self._read_header(csv_file)
            self._read_data(csv_file)

    def _read_header(self, csv_file: any):
        csv_reader = csv.reader(csv_file, delimiter=',', quotechar='"')
        header_line1 = next(csv_reader)
        header_line2 = next(csv_reader)
        next(csv_reader)
        header_line_4 = next(csv_reader)

        self._varies_by_block = int(header_line1[1]) >= 1
        self._units = header_line1[3].strip()
        stage_type = int(header_line1[4])
        self._case_initial_stage = int(header_line1[5])
        self._initial_year = int(header_line1[6])
        if stage_type in (self.STAGE_TYPE_WEEKLY, self.STAGE_TYPE_MONTHLY,
                          self.STAGE_TYPE_13MONTHLY):
            self._stage_type = stage_type
        else:
            raise ValueError(f"Invalid stage type: {stage_type}")

        self._varies_by_scenario = int(header_line2[1]) == 1
        self._agents = tuple(map(lambda x: x.strip(), header_line_4[3:]))
        self._stages = 0
        self._scenarios = 0

    def _read_data(self, csv_file: any):
        csv_reader = csv.reader(csv_file, delimiter=',', quotechar='"')
        self.__data = {}
        self._max_stage = 0
        self._min_stage = 999999
        for line in csv_reader:
            key = tuple(map(int, line[:3]))
            values = tuple(map(float, line[3:]))
            self.__data[key] = values
            # Update limits
            stage = key[0]
            scenario = key[1]
            block = key[2]
            if stage > self._max_stage:
                self._max_stage = stage
            if stage < self._min_stage:
                self._min_stage = stage
            if scenario > self._scenarios:
                self._scenarios = scenario
            if stage not in self.__max_blocks_per_stage or \
                    block > self.__max_blocks_per_stage[stage]:
                self.__max_blocks_per_stage[stage] = block

        self._stages = self._max_stage - self._min_stage + 1

        if self._is_hourly_data():
            self._hour_or_block = self.BLOCK_TYPE_HOUR
        else:
            self._hour_or_block = self.BLOCK_TYPE_BLOCK

    def _is_hourly_data(self) -> bool:
        if self._stage_type == self.STAGE_TYPE_WEEKLY:
            max_blocks = [blocks for stage, blocks in
                          self.__max_blocks_per_stage.items()]
            for blocks in max_blocks:
                if blocks != 168:
                    return False
            return True
        if self._stage_type == self.STAGE_TYPE_MONTHLY:
            for stage, blocks in self.__max_blocks_per_stage.items():
                if stage in (1, 3, 5, 7, 8, 10, 12):
                    if blocks != 744:
                        return False
                elif stage in (4, 6, 9, 11):
                    if blocks != 720:
                        return False
                else:
                    if blocks != 672:
                        return False
            return True

        return self._stage_type == self.STAGE_TYPE_WEEKLY

    def blocks(self, stage: int) -> int:
        """Number of blocks for a given stage. 1-based stage."""
        istage = stage - self._min_stage + 1
        if self._varies_by_block != 0:
            return self.__max_blocks_per_stage[istage]
        return 1

    def read(self, stage: int, scenario: int, block: int) -> tuple:
        """
        Read data of a given stage, scenario, and block. Returns a list with
        values for each agent.

        Raises IndexError if stage, scenario, or block is out of bounds.

        Non thread-safe.
        """
        self._check_indexes(stage, scenario, block)
        return self.__data[(stage, scenario, block)]


@contextmanager
def open_bin(file_path: Union[str, pathlib.Path], **kwargs):
    """
    Open SDDP binary files (.hdr and .bin) for reading provided a file path
    for one of them or a file path without extension. Yields a
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


@contextmanager
def open_csv(file_path: Union[str, pathlib.Path], **kwargs):
    """
    Open SDDP csv result files (.csv) for reading provided a file path.
    Yields a SddpCsvReader class.
    """
    obj = CsvReader()
    obj.open(file_path, **kwargs)
    yield obj
    obj.close()


def _get_agent_index_filter(agents: Tuple[str], filter_agents: Tuple[str]) -> Tuple[int, ...]:
    if len(filter_agents) == 0:
        return tuple(range(len(agents)))
    else:
        return tuple(agents.index(agent) for agent in filter_agents)


def load_as_dataframe(file_path: Union[str, pathlib.Path], **kwargs) -> Optional[pd.DataFrame]:
    use_multi_index = kwargs.get('multi_index', True)
    index_format = kwargs.get('index_format', 'default')
    filter_agents = kwargs.get('filter_agents', [])
    filter_stages = kwargs.get('filter_stages', [])
    filter_blocks = kwargs.get('filter_blocks', [])
    filter_scenarios = kwargs.get('filter_scenarios', [])

    filter_agents_set = tuple(agent.strip().lower() for agent in filter_agents)

    if len(filter_stages) > 0:
        def test_stage(_stage: int) -> bool:
            return _stage in filter_stages
    else:
        def test_stage(_stage: int) -> bool:
            return True

    if len(filter_blocks) > 0:
        def test_block(_block: int) -> bool:
            return _block in filter_blocks
    else:
        def test_block(_block: int) -> bool:
            return True

    if len(filter_scenarios) > 0:
        def test_scenario(_scenario: int) -> bool:
            return _scenario in filter_scenarios
    else:
        def test_scenario(_scenario: int) -> bool:
            return True

    if not _HAS_PANDAS:
        raise ImportError("pandas is not available.")

    file_path = str(file_path)
    _, ext = os.path.splitext(file_path)
    if ext.lower() == ".csv":
        open_fn = open_csv
    else:
        open_fn = open_bin
    with open_fn(file_path, **kwargs) as graf_file:
        data = []
        index_values = []
        block_or_hour = BinReader.BLOCK_DESCRIPTION[
            graf_file.hour_or_block]

        if index_format == 'default':
            index_columns = ('stage', 'scenario', block_or_hour)

            def get_key(_stage: int, _scenario: int, _block: int) -> Tuple[int, int, int]:
                return _stage, _scenario, _block
        elif index_format == 'period':
            if graf_file.stage_type == _GrafReaderBase.STAGE_TYPE_MONTHLY:
                month_or_week = 'month'
                max_periods = 12
            else:
                month_or_week = 'week'
                max_periods = 52
            index_columns = ('year', month_or_week, 'scenario', block_or_hour)

            def get_key(_stage: int, _scenario: int, _block: int) -> Tuple[int, int, int, int]:
                year = (_stage + graf_file.initial_stage - 2) // max_periods + graf_file.initial_year
                _month_or_week = (_stage + graf_file.initial_stage - 2) % max_periods + 1
                return year, _month_or_week, _scenario, _block

        if len(filter_agents_set) == 0:
            df_agents = graf_file.agents

            def filter_agents(values: Tuple[float]) -> Tuple[float]:
                return values
        else:
            original_agents = graf_file.agents
            lower_case_agents = tuple(agent.strip().lower()
                                  for agent in original_agents)
            agents_index = _get_agent_index_filter(lower_case_agents,
                                                   filter_agents_set)
            df_agents = tuple(original_agents[i] for i in agents_index)

            def filter_agents(values: Tuple[float]) -> Tuple[float, ...]:
                return tuple(values[i] for i in agents_index)

        if use_multi_index:
            def append_row(_stage: int, _scenario: int, _block: int):
                data.append(filter_agents(graf_file.read(_stage, _scenario,
                                                         _block)))
                index_values.append(get_key(_stage, _scenario, _block))
        else:
            def append_row(_stage: int, _scenario: int, _block: int):
                data.append(get_key(_stage, _scenario, _block) +
                            filter_agents(graf_file.read(_stage, _scenario,
                                                         _block)))

        for stage in range(graf_file.min_stage, graf_file.max_stage + 1):
            total_blocks = graf_file.blocks(stage)
            if test_stage(stage):
                for scenario in range(1, graf_file.scenarios + 1):
                    if test_scenario(scenario):
                        for block in range(1, total_blocks + 1):
                            if test_block(block):
                                append_row(stage, scenario, block)
    if use_multi_index:
        index = pd.MultiIndex.from_tuples(index_values,
                                          names=index_columns)
        return pd.DataFrame(data, index=index, columns=df_agents)
    else:
        return pd.DataFrame(data, columns=index_columns + df_agents)
