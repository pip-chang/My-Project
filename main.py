from pydoc import synopsis
from tracemalloc import start
import jsonlines
import json
import os
import glob
import pandas as pd
import numpy as np
from pathlib import Path
from Reddit_Corpus.process_data import DataHandler, DataProcessor

### Settings

# file settings
INPUT_DIR = (
    "/Users/pipchang/Documents/VSC/Projects/Corpus_Linguistics/Reddit_Corpus/tata_com"
)
OUTPUT_DIR = "/Users/pipchang/Documents/VSC/Projects/Corpus_Linguistics/Reddit_Corpus/tata_output/220701_221130_(N=0.5)_processed_com"
AUTHOR_DICT = "/Users/pipchang/Documents/VSC/Projects/Corpus_Linguistics/Reddit_Corpus/tata_output/220101_221130/author_dict_comments_220101_221130.json"
ID_DICT = "/Users/pipchang/Documents/VSC/Projects/Corpus_Linguistics/Reddit_Corpus/tata_output/140101_221130/id_dict_submissions_140101_221130.json"


# mode setting - "comments" or "submissions"
MODE = "comments"

# items settings
N_ITEMS = None

# time period settings
TIME_PERIOD = ("20220701 00:00:00", "20221130 23:59:59")

# if split time period into chunks of N months
CHUNK_N_MONTHS = 0.5

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

    p.export_processed_jsonl(
        author_dict_path=AUTHOR_DICT,
        id_dict_path=ID_DICT,
        n_items=N_ITEMS,
        time_period=TIME_PERIOD,
        chunk=CHUNK_N_MONTHS,
        default=True,
        custom_keys=None,
        custom_filename=None,
    )

    # p.export_poster_stats(
    #     author_dict_path=AUTHOR_DICT,
    #     minpost=1,
    #     n_items=N_ITEMS,
    #     time_period=TIME_PERIOD,
    #     custom_filename=None,
    # )

    ## general info
    # d = {1: {}, 2: {}}
    # for line in p.gen:
    #     a = line.get("collapsed_because_crowd_control")
    #     b = line.get("collapsed_reason_code")
    #     if a not in d[1].keys():
    #         d[1][a] = 1
    #     else:
    #         d[1][a] += 1
    #     if b not in d[2].keys():
    #         d[2][b] = 1
    #     else:
    #         d[2][b] += 1

    # print(d)  # "{1: {None: 1000000}, 2: {None: 949102, 'DELETED': 43380, 'LOW_SCORE': 7518}}"


if False:  # get rid pf duplicates in submissions (2020-06-01 - 2022-12-01)
    # >>> start = 301001 - created_utc = 1634499927
    # >>> end = 328000 - created_utc = 1640585244)
    file_path = "/Users/pipchang/Documents/VSC/Projects/Corpus_Linguistics/Reddit_Corpus/tata/tata_submissions_1.jsonl"
    output_path = "/Users/pipchang/Documents/VSC/Projects/Corpus_Linguistics/Reddit_Corpus/tata/tata_submissions_1_nodup.jsonl"
    with open(output_path, mode="a+", encoding="utf-8") as f:
        with jsonlines.open(file_path) as reader:
            for line in reader:
                if line["created_utc"] > 1640585244:
                    newline = line
                    json_record = json.dumps(newline, ensure_ascii=False)
                    f.write(json_record + "\n")
