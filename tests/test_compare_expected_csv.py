import os
import unittest
import pandas as pd
import pandas.testing
import psr.graf


_DEBUG_PRINT = False

_DEFAULT_TOLERANCE = 1e-05

def get_sample_folder_path():
    # type: () -> str
    return os.path.join(os.path.dirname(os.path.dirname(__file__)),
                        "sample_data")


def get_test_folder_path():
    # type: () -> str
    return os.path.join(os.path.dirname(__file__), "test_data")


def load_csv_as_dataframe(csv_file_path, **kwargs):
    # type: (str, dict) -> pd.DataFrame
    return psr.graf.load_as_dataframe(csv_file_path, **kwargs)


def load_common_csv_as_dataframe(csv_file_path, **kwargs):
    # type: (str, dict) -> pd.DataFrame
    return pd.read_csv(csv_file_path, **kwargs)


def assert_df_equal(df1, df2, rtol):
    # type: (pd.DataFrame, pd.DataFrame, float) -> None
    # Test using a tolerance of 1e-6
    pandas.testing.assert_frame_equal(df1, df2, rtol=rtol)


class CompareExpectedCsv(unittest.TestCase):
    def setUp(self):
        self.sample_file_name = "coster.hdr"
        self.encoding = 'utf-8'
        self.index_format = 'default'
        self.multi_index = True
        self.filter_agents = []
        self.filter_stages = []
        self.filter_scenarios = []
        self.filter_blocks = []
        self.tolerance = _DEFAULT_TOLERANCE

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
                                     encoding=self.encoding,
                                     multi_index=self.multi_index,
                                     index_format=self.index_format,
                                     filter_agents=self.filter_agents,
                                     filter_stages=self.filter_stages,
                                     filter_scenarios=self.filter_scenarios,
                                     filter_blocks=self.filter_blocks)

    def get_sample_df(self):
        # type: () -> pd.DataFrame
        if _DEBUG_PRINT:
            print(self._get_sample_file_path())
        return psr.graf.load_as_dataframe(self._get_sample_file_path(),
                                          encoding=self.encoding,
                                          multi_index=self.multi_index,
                                          index_format=self.index_format,
                                          filter_agents=self.filter_agents,
                                          filter_stages=self.filter_stages,
                                          filter_scenarios=self.filter_scenarios,
                                          filter_blocks=self.filter_blocks)

    def test_compare_files(self):
        test_df = self.get_test_df()
        sample_df = self.get_sample_df()
        if _DEBUG_PRINT:
            print(test_df.compare(sample_df))
        assert_df_equal(test_df, sample_df, self.tolerance)


class CompareExpectedCommonCsv(CompareExpectedCsv):
    def setUp(self):
        self.sample_file_name = "gerter.hdr"
        self.encoding = 'utf-8'
        self.index_format = 'default'
        self.multi_index = False
        self.filter_agents = ["Thermal 3", "Thermal 2"]
        self.filter_stages = [1, 12]
        self.filter_scenarios = [1, 2, 3]
        self.filter_blocks = [1, ]
        self.tolerance = _DEFAULT_TOLERANCE

    def get_test_df(self):
        # type: () -> pd.DataFrame
        if _DEBUG_PRINT:
            print(self._get_test_csv_file_path())
        return load_common_csv_as_dataframe(self._get_test_csv_file_path(),
                                            encoding=self.encoding)


class CompareCosterLatin1Csv(CompareExpectedCsv):
    def setUp(self):
        self.sample_file_name = "coster_latin1.hdr"
        self.encoding = 'latin-1'
        self.index_format = 'default'
        self.multi_index = True
        self.filter_agents = []
        self.filter_stages = []
        self.filter_scenarios = []
        self.filter_blocks = []
        self.tolerance = _DEFAULT_TOLERANCE


class CompareDemandCsvMultiIndex(CompareExpectedCsv):
    def setUp(self):
        self.sample_file_name = "demand.hdr"
        self.encoding = 'utf-8'
        self.index_format = 'default'
        self.multi_index = True
        self.filter_agents = []
        self.filter_stages = []
        self.filter_scenarios = []
        self.filter_blocks = []
        self.tolerance = _DEFAULT_TOLERANCE


class CompareDemandCsvSingleIndex(CompareExpectedCsv):
    def setUp(self):
        self.sample_file_name = "demand.hdr"
        self.encoding = 'utf-8'
        self.index_format = 'default'
        self.multi_index = False
        self.filter_agents = []
        self.filter_stages = []
        self.filter_scenarios = []
        self.filter_blocks = []
        self.tolerance = _DEFAULT_TOLERANCE


class CompareDataWithoutScenarios(CompareExpectedCsv):
    def setUp(self):
        self.sample_file_name = "duraci.hdr"
        self.encoding = 'utf-8'
        self.index_format = 'default'
        self.multi_index = True
        self.filter_agents = []
        self.filter_stages = []
        self.filter_scenarios = []
        self.filter_blocks = []
        self.tolerance = _DEFAULT_TOLERANCE


class CompareDataWithNegativeStages(CompareExpectedCsv):
    def setUp(self):
        self.sample_file_name = "inflow"
        self.encoding = 'utf-8'
        self.index_format = 'default'
        self.multi_index = True
        self.filter_agents = []
        self.filter_stages = []
        self.filter_scenarios = []
        self.filter_blocks = []
        self.tolerance = 1e-03


if __name__ == '__main__':
    unittest.main()
