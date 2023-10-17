class Config:
    pass


class OutputConfig(Config):
    OUTPUT_DIR = '/cpfs/output/%s' % Config.NAME1
    QUERY_OUTPUT_DIR = '%s/card/query' % OUTPUT_DIR
    OTHERS_OUTPUT_DIR = '%s/other' % OUTPUT_DIR
    DATAFRAME_OUTPUT_PATH = '%s/other/analysis_result.pkl' % OUTPUT_DIR
    OUTPUT_ANNOTATION_PATH = '%s/card/annotation' % OUTPUT_DIR


class InputConfig(Config):
    JSON_PATH = "/data_path/nerf_test.txt"
    JSON_TYPE = "txt"
    DATA_TYPE = "nerf"

