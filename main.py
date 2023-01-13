import jsonlines
import json
import os
import glob
from process_data import DataHandler, DataProcessor

### Settings

# file settings
INPUT_DIR = ("./example-data/comments")
OUTPUT_DIR = "./output"
AUTHOR_DICT = "./example-data/author_dict_submissions_220101_220331.json"
ID_DICT = "./example-data/id_dict_submissions_220101_220331.json"


# mode setting - "comments" or "submissions"
MODE = "comments"

# items settings
N_ITEMS = None

# time period settings
TIME_PERIOD = ("20220101 00:00:00", "20220331 23:59:59")

# if split time period into chunks of N months
CHUNK_N_MONTHS = None

if __name__ == "__main__":
    p = DataProcessor(
        input_path=INPUT_DIR,
        output_directory=OUTPUT_DIR,
        mode=MODE,
    )
    # p.export_author_dict(
    #     n_items=N_ITEMS,
    #     time_period=TIME_PERIOD,
    #     chunk=CHUNK_N_MONTHS,
    # )

    # p.export_id_dict(
    #     n_items=N_ITEMS,
    #     time_period=TIME_PERIOD,
    #     chunk=CHUNK_N_MONTHS,
    # )

    # p.export_processed_jsonl(
    #     author_dict_path=AUTHOR_DICT,
    #     id_dict_path=ID_DICT,
    #     n_items=N_ITEMS,
    #     time_period=TIME_PERIOD,
    #     chunk=CHUNK_N_MONTHS,
    #     default=True,
    #     custom_keys=None,
    #     custom_filename=None,
    # )

    # p.export_poster_stats(
    #     author_dict_path=AUTHOR_DICT,
    #     minpost=1,
    #     n_items=N_ITEMS,
    #     time_period=TIME_PERIOD,
    #     custom_filename=None,
    # )

    


