import os
import shutil
import sys
import unittest
from unittest.mock import patch

import pyarrow.parquet as pq

sys.path.append(".")
from bin2parquet import main as bin2parquet

OUT_DIR = "tests/output"


def out_path(variable:str) -> str:
    return os.path.join(OUT_DIR, f"{variable}.parquet")


class TestBin2Parquet(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if os.path.exists(OUT_DIR):
            shutil.rmtree(OUT_DIR)
        args = [
            'bin2parquet', "tests/input", 'objcop', 'inflow', 'sumcir',
            '--output-dir', OUT_DIR]
        with patch("sys.argv", args):
            bin2parquet()

    def test_objcop_produced(self):
        self.assertTrue(os.path.exists(out_path("objcop")))

    def test_inflow_produced(self):
        self.assertTrue(os.path.exists(out_path("inflow")))

    def test_objcop_has_data(self):
        df = pq.read_table(out_path("objcop")).to_pandas()
        for col in ["stage", "scenario", "block"]:
            self.assertIn(col, df.columns)
        self.assertGreater(df.shape[0], 0)    
        self.assertGreater(df.shape[1], 3)    

    def test_objcop_has_correct_data(self):
        test_df = pq.read_table(out_path("objcop")).to_pandas()
        ref_df = pq.read_table("tests/ref/objcop.parquet").to_pandas()
        self.assertTrue(test_df.equals(ref_df))

    def test_inflow_has_data(self):
        df = pq.read_table(out_path("inflow")).to_pandas()
        for col in ["stage", "scenario", "block"]:
            self.assertIn(col, df.columns)    
        self.assertGreater(df.shape[0], 0)    
        self.assertGreater(df.shape[1], 3)

    def test_multi_block(self):
        """testing a small multi block per stage bin file"""
        test_df = pq.read_table(out_path("sumcir")).to_pandas()
        ref_df = pq.read_table("tests/ref/sumcir.parquet").to_pandas()
        self.assertTrue(test_df.equals(ref_df))



if __name__ == "__main__":
    unittest.main()

