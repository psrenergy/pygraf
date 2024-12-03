# This file is almost entirely copied from "parquet_example.py" from the psr.graf
# package.  Changes include a slightly different API for the commandline script
# removal of the csv functionality, and a work-around for the inflow files, which
# cause graf_to_parquet to fail due to stage starting from 0.

# Converts a SDDP result binary file to Apache Parquet file format.
#from __future__ import print_function
import argparse
from contextlib import contextmanager
import logging
import os
import queue
import struct
import threading

import numpy as np
from psr.graf import BinReader, load_as_dataframe
import pyarrow as pa
import pyarrow.parquet as pq

_WORD = 4

# these variables are needed for TPM clause 51 and 52
DEFAULT_VARS = [
    'cmgbus', 'defbus', 'gerter', 'gergnd', 'gerbat', 'gerhid', 'coster', 
    'cosco2', 'demxba', 'usedcl'
    ]

# Change this number to optimize the number of stages read/written
# in parquet files.  Not seeing much motivation to change this for now.
_stage_chunk_size = 10


class MyBinReader(BinReader):    
    """A modified version of the psr.graf.BinReader class with an alternative to
     the read_blocks method, that avoids some expensive for loops by using numpy
     to reshape the incoming stream into a 2D array. """

    def read_blocks_as_array(self, stage:int, scenario:int) -> np.ndarray:
        """
        Read data of a given stage and scenario. Returns a 2D numpy array with
        dimensions (blocks, agents).

        Raises IndexError if stage or scenario is out of bounds.        
        """
        ## begin copy from psr.graf.BinReader.read_blocks -----------
        self._check_indexes(stage, scenario)
        i_stage = stage - 1
        # self.__seek in the original (name mangling of private method)
        self._BinReader__seek(i_stage, scenario, 1)

        agents = len(self._agents)
        blocks = self._bin_offsets[i_stage + 1] - self._bin_offsets[i_stage]
        count = blocks * agents

        fmt = "{}f".format(count)
        all_values = struct.unpack(
            fmt, 
            # self.__bin_file_handler.read in the original (name mangling of private method)
            self._BinReader__bin_file_handler.read(_WORD * count)) 
        len_per_agent = int(len(all_values) / agents) # I cant see why this is not just blocks
        # end copy from psr.graf.BinReader.read_blocks --------------

        # leaving the original code here for reference
        # lists = []
        # for i_agent in range(agents):
        #     values = [0.0] * len_per_agent
        #     for i_value in range(len_per_agent):
        #         values[i_value] = all_values[i_agent + i_value * agents]
        #     lists.append(values)
        # return lists

        return np.array(all_values, dtype=np.float32).reshape((len_per_agent, agents))
    

# just a copy of psr.graf.open_bin that uses the MyBinReader class
@contextmanager
def my_open_bin(file_path: str, **kwargs):
    reader = MyBinReader()
    reader.open(file_path, **kwargs)
    yield reader
    reader.close()

# copied from the parquet_example.py file from the psr.graf package
# used to break the list of stages into chunks
# chunk_size would be better named num_chunks
def chunkfy(data:list, chunk_size:int) -> list:
    n = len(data)
    l = data
    k = chunk_size
    return [l[i * (n // k) + min(i, n % k):
              (i + 1) * (n // k) + min(i + 1, n % k)] for i in range(k)]


def inflow_to_parquet(graf_file_path:str, parquet_file_path: str) -> None:
    """This a special case for inflow bin files which cause the graf_to_parquet
    function to fail due to the stage starting from 0."""
    # inflow tables are relatively small so we can get away with loading the
    # entire table into memory at once.
    df = load_as_dataframe(graf_file_path)
    df.reset_index().to_parquet(parquet_file_path)


# this started as a copy of the graf_to_parquet function from the parquet_example.py
# file from the psr.graf package, but has been modified to use the MyBinReader
# class, get speed and memmory improvements from using numpy instead of for loops and lists,
# and to use a queue to write the parquet file in the background.
def graf_to_parquet(graf_file_path:str, parquet_file_path:str) -> None:

    # parquet will be written to this temporary file before moving to the final
    # destination
    part_parquet_file_path = parquet_file_path + '.part'
    logging.info(f"writing to temporary file {part_parquet_file_path}")
    
    with my_open_bin(graf_file_path) as graf_file:
        # The code below specifies the table layout.
        fields = [
            pa.field('stage', pa.int64()),
            pa.field('scenario', pa.int64()),
            pa.field('block', pa.int64())
        ]
        fields.extend([pa.field(agent, pa.float32())
                       for agent in graf_file.agents])

        first_chunk = True
        for stage_chunk in chunkfy(
            list(range(graf_file.min_stage,graf_file.max_stage + 1)),
                        _stage_chunk_size):
            stages = []  # Stage number column.
            scenarios = []  # Scenario number column.
            blocks = []  # Blocks number column.
            agents = []  # Stores a 2D numpy array for each chunk.
            
            for stage in stage_chunk:
                for scenario in range(1, graf_file.scenarios + 1):
                    data = graf_file.read_blocks_as_array(stage, scenario)
                    total_blocks = data.shape[0]
                    current_blocks = list(range(1, total_blocks + 1))

                    stages.extend([stage] * total_blocks)
                    scenarios.extend([scenario] * total_blocks)
                    blocks.extend(current_blocks)
                    agents.append(data)
            
            agents = np.concatenate(agents, axis=0)

            # create a pyarrow table from the data
            arrays = [
                pa.array(stages),
                pa.array(scenarios),
                pa.array(blocks)
            ]
            arrays.extend([pa.array(agents[:, i]) for i in range(agents.shape[1])])
            table = pa.Table.from_arrays(arrays=arrays, schema=pa.schema(fields))

            # if this is the first chunk, initialize the parquet writer and the
            # write queue
            if first_chunk:
                parquet_writer = pq.ParquetWriter(part_parquet_file_path, table.schema)
                
                # setting maxsize to 1 to guard against memory use climbing if
                # the writing thread falls behind.
                write_queue = queue.Queue(maxsize=1)
                
                def writer():
                    while True:
                        this_table = write_queue.get()
                        parquet_writer.write_table(this_table)
                        write_queue.task_done()

                threading.Thread(target=writer, daemon=True).start()
                
                first_chunk = False
            
            write_queue.put(table)

        # wait for the write queue to finish
        write_queue.join()
        
        parquet_writer.close()
        logging.info(f"successfully finished writing to {part_parquet_file_path}")

    # Move the temporary file to the final destination.
    logging.info(f"moving {part_parquet_file_path} to {parquet_file_path}")
    os.rename(part_parquet_file_path, parquet_file_path)


def main():
    # Read file name from command line arguments
    # - or use sample data if not provided.
    parser = argparse.ArgumentParser(
        description='Converts a SDDP result binary file to Apache Parquet '
                    'file format.')
    parser.add_argument(
        'input_dir', type=str, help='directory of SDDP binary outputs')
    parser.add_argument(
        "vars", nargs = "*", help = "variables to convert", default=DEFAULT_VARS)
    parser.add_argument(
        '--output-dir', type=str, help='directory for output Parquet files', 
        default="./")
    args = parser.parse_args()

    log_path = 'bin2parquet.log'
    print(f"Log messages going to {log_path}")
    logging.basicConfig(
        level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s',
        filename=log_path, filemode='w')

    if not os.path.exists(args.output_dir):
        print(f"created output directory; {args.output_dir}")
        os.makedirs(args.output_dir)

    for var in args.vars:
        bin_path = os.path.join(args.input_dir, f"{var}.hdr")
        parquet_path = os.path.join(args.output_dir, f'{var}.parquet')
        if os.path.exists(parquet_path):
            msg = f"output file {parquet_path} already exists. Skipping"
            print(msg)
            logging.info(msg)
            continue
        try:
            msg = f"converting {bin_path} to {parquet_path}"
            print(msg)
            logging.info(msg)
            if var == "inflow":
                inflow_to_parquet(bin_path, parquet_path)
            else:
                graf_to_parquet(bin_path, parquet_path)
        except Exception as err:
            msg = f"conversion for variable {var} failed: {err.__class__.__name__}, {err}"
            print(msg)
            logging.warning(msg)

    logging.info("Done")
        

if __name__ == "__main__":
    main()