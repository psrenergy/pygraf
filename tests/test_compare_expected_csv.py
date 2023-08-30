import os
import unittest
import pandas as pd
import pandas.testing
import psr.graf


_DEBUG_PRINT = False


def get_sample_folder_path():
    # type: () -> str
    return os.path.join(os.path.dirname(__file__), "..", "sample_data")


def get_test_folder_path():
    # type: () -> str
    return os.path.join(os.path.dirname(__file__), "test_data")


def load_csv_as_dataframe(csv_file_path, **kwargs):
    # type: (str, dict) -> pd.DataFrame
    return psr.graf.load_as_dataframe(csv_file_path, **kwargs)


def assert_df_equal(df1, df2):
    # type: (pd.DataFrame, pd.DataFrame) -> None
    pandas.testing.assert_frame_equal(df1, df2)


class CompareExpectedCsv(unittest.TestCase):
    def setUp(self):
        self.sample_file_name = "coster.hdr"
        self.encoding = 'utf-8'

    def _get_sample_file_path(self):
        # type: () -> str
        return os.path.join(get_sample_folder_path(), self.sample_file_name)

    def _get_test_csv_file_path(self):
        # type: () -> str
        base_file_name = os.path.splitext(self.sample_file_name)[0]
        return os.path.join(get_test_folder_path(), base_file_name + ".csv")

    def get_test_df(self):
        # type: () -> pd.DataFrame
        if _DEBUG_PRINT:
            print(self._get_test_csv_file_path())
        return load_csv_as_dataframe(self._get_test_csv_file_path(),
                                     encoding=self.encoding)

    def get_sample_df(self):
        # type: () -> pd.DataFrame
        if _DEBUG_PRINT:
            print(self._get_sample_file_path())
        return psr.graf.load_as_dataframe(self._get_sample_file_path(),
                                          encoding=self.encoding)

    def test_compare_files(self):
        test_df = self.get_test_df()
        sample_df = self.get_sample_df()
        if _DEBUG_PRINT:
            print(test_df.compare(sample_df))
        assert_df_equal(test_df, sample_df)


class CompareCosterLatin1Csv(CompareExpectedCsv):
    def setUp(self):
        self.sample_file_name = "coster_latin1.hdr"
        self.encoding = 'latin-1'


class CompareDemandCsv(CompareExpectedCsv):
    def setUp(self):
        self.sample_file_name = "demand.hdr"
        self.encoding = 'utf-8'


class CompareSingleBinary(CompareExpectedCsv):
    def setUp(self):
        self.sample_file_name = "scen_hourinflow_w.dat"
        self.encoding = 'utf-8'


if __name__ == '__main__':
    unittest.main()
