import json
from psaw import PushshiftAPI
import datetime as dt
from tqdm import tqdm


def get_start_epoch(path=None, time=None) -> int:
    """ """
    if path:
        with open(path, "r") as f:
            lines = f.read().splitlines()
            last_line = lines[-1]
            start_epoch = int(last_line)
    if time:
        if type(time) is str:
            dt_object = dt.datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
            start_epoch = int(dt_object.timestamp())
        if type(time) is dt.datetime:
            start_epoch = int(time.timestamp())
    return start_epoch


def append_last_epoch(output_path, last_epoch: int):
    with open(output_path, "a") as f:
        f.write(str(last_epoch) + "\n")


def get_comments_by_subreddit(
    api,
    start_epoch: int,
    subreddit: str,
    output_path,
):
    gen = api.search_comments(
        subreddit=subreddit,
        sort="asc",
        after=start_epoch,
        metadata=True,
    )

    max_response_cache = 1000
    max_json_lines = 100000
    data = []
    lines = 0

    for c in tqdm(gen):
        data.append(c.d_)
        lines += 1
        # shards_status = api.metadata_.get("shards")
        try:
            if len(data) >= max_response_cache:
                dump_jsonl(data, output_path)
                data = []
            if lines >= max_json_lines:
                last_epoch = c.d_["created_utc"]
                break
        except StopIteration:
            pass
            last_epoch = c.d_["created_utc"]
    return last_epoch


def get_submissions_by_subreddit(
    api,
    start_epoch: int,
    subreddit: str,
    output_path,
):
    gen = api.search_submissions(
        subreddit=subreddit,
        sort="asc",
        after=start_epoch,
        metadata=True,
    )

    max_response_cache = 10
    max_json_lines = 100000
    data = []
    lines = 0

    for c in tqdm(gen):
        data.append(c.d_)
        lines += 1
        # shards_status = api.metadata_.get("shards")
        try:
            if len(data) >= max_response_cache:
                dump_jsonl(data, output_path)
                data = []
            if lines >= max_json_lines:
                last_epoch = c.d_["created_utc"]
                break
        except StopIteration:
            pass
            last_epoch = c.d_["created_utc"]
    return last_epoch


def dump_jsonl(data, output_path, append=True):
    """
    Write list of objects to a JSON lines file.
    """
    mode = "a+" if append else "w"
    with open(output_path, mode, encoding="utf-8") as f:
        for line in data:
            json_record = json.dumps(line, ensure_ascii=False)
            f.write(json_record + "\n")


if __name__ == "__main__":
    api = PushshiftAPI()
    get = "comments"
    if get == "comments":
        last_epoch_path = "/Users/pipchang/Documents/VSC/Projects/Corpus_Linguistics/Reddit_Corpus/tata_com/last_epoch_comments.txt"
        start_epoch = get_start_epoch(last_epoch_path)
        subreddit = "TooAfraidToAsk"
        output_path = "/Users/pipchang/Documents/VSC/Projects/Corpus_Linguistics/Reddit_Corpus/tata_com/tata_comments_18.jsonl"
        last_epoch = get_comments_by_subreddit(api, start_epoch, subreddit, output_path)
        append_last_epoch(last_epoch_path, last_epoch)
    if get == "submissions":
        last_epoch_path = "/Users/pipchang/Documents/VSC/Projects/Corpus_Linguistics/Reddit_Corpus/tata_post/last_epoch_submissions.txt"
        start_epoch = get_start_epoch(last_epoch_path)
        subreddit = "TooAfraidToAsk"
        output_path = "/Users/pipchang/Documents/VSC/Projects/Corpus_Linguistics/Reddit_Corpus/tata_post/tata_submissions_3.jsonl"
        last_epoch = get_submissions_by_subreddit(
            api, start_epoch, subreddit, output_path
        )
        append_last_epoch(last_epoch_path, last_epoch)
