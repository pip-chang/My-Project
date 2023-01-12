import nltk
import json
import jsonlines
import glob
import datetime
import pandas as pd
import numpy as np
from tqdm import tqdm
from pathlib import Path


class DataHandler:
    """
    return a generator that yields each line of all jsonl files in a given folder
    """

    def __init__(
        self,
        input_path: str,
    ):
        # running status
        self.running = False

        # path(s)
        if Path(input_path).is_dir():
            self.jsonl_paths = sorted(list(Path(input_path).glob("*.jsonl")))
            assert (
                len(self.jsonl_paths) > 0
            ), f"{input_path} does not contain any jsonl files."
        else:
            assert (
                Path(input_path).suffix == ".jsonl"
            ), f"{input_path} is not a directory or jsonl file."
            self.jsonl_paths = [input_path]

    @staticmethod
    def get_epoch(time) -> int:
        if isinstance(time, str):
            time = datetime.datetime.strptime(time, "%Y%m%d %H:%M:%S")
        epoch = int(time.timestamp())
        return epoch

    def get_generator(
        self,
        n_items: int = None,
        time_period: tuple = None,
    ):
        """
        :param time_period
        :type tuple of two strings in format "%Y%m%d %H:%M:%S" - e.g. ("20180317 01:23:45", "20190317 01:23:49")

        :return generator value - jsonline {}
        """
        if time_period:
            start = time_period[0]
            end = time_period[1]
            start_epoch = self.get_epoch(start)
            end_epoch = self.get_epoch(end)
            counter = 0
            for jsonl_path in self.jsonl_paths:
                with jsonlines.open(jsonl_path) as reader:
                    generator = reader.iter()
                    self.running = True
                    for line in generator:
                        utc = line.get("created_utc")
                        if utc > start_epoch and utc <= end_epoch:
                            yield line
                            counter += 1
                        if utc > end_epoch:
                            self.running = False
                            generator.close()

        if n_items:
            for jsonl_path in self.jsonl_paths:
                counter = 0
                with jsonlines.open(jsonl_path) as reader:
                    generator = reader.iter()
                    self.running = True
                    for line in generator:
                        yield line
                        counter += 1
                        if counter == n_items:
                            self.running = False
                            generator.close()

        else:
            for jsonl_path in self.jsonl_paths:
                counter = 0
                with jsonlines.open(jsonl_path) as reader:
                    generator = reader.iter()
                    self.running = True
                    for line in generator:
                        try:
                            yield line
                            counter += 1
                        finally:
                            self.running = False


class DataProcessor(DataHandler):
    """
    process input data from the generator and return result
    """

    def __init__(
        self,
        input_path: str,
        output_directory: str,
        mode: str,
    ):
        """
        :para mode
        :type either "comments" or "submissions"
        """
        # inherit
        super().__init__(input_path)

        # output directory
        assert (
            Path(output_directory).is_dir(),
            f"{output_directory} is not a directory.",
        )
        self.output_directory = output_directory

        # mode
        assert mode in [
            "submissions",
            "comments",
        ], f"{mode} must be 'comments' or 'submissions'."
        self.mode = mode

    @staticmethod
    def sort_dict(d):
        d = {k: v for k, v in sorted(d.items(), key=lambda item: item[1], reverse=True)}
        return d

    @staticmethod
    def get_periods(time_period: tuple, n) -> list:
        """
        return a list of tuples of time periods
        n can be integers or 0.5
        """
        begin_date = time_period[0]
        end_date = time_period[1]
        if isinstance(n, int):
            months = (
                pd.date_range(begin_date, end_date, freq="MS")
                .strftime("%Y%m%d %H:%M:%S")
                .tolist()
            )
        else:
            months = (
                pd.date_range(begin_date, end_date, freq="SMS")
                .strftime("%Y%m%d %H:%M:%S")
                .tolist()
            )
            n = 1
        rep = len(months) // n
        index = []
        for i in range(rep + 1):
            index.append(i * n)

        periods = []
        for i in range(rep):
            if i == (rep - 1):
                start_index = index[i]
                period = (months[start_index], end_date)
                periods.append(period)
            else:
                start_index = index[i]
                end_index = index[i + 1]
                period = (months[start_index], months[end_index])
                periods.append(period)
        return periods

    @staticmethod
    def get_filenames(periods):
        """
        return either a list of strings, or a string
        """
        if isinstance(periods, list):
            filenames = []
            for p in periods:
                begin = p[0][2:8]
                end = p[1][2:8]
                filename = f"{begin}_{end}"
                filenames.append(filename)
            return filenames
        else:
            begin = periods[0][2:8]
            end = periods[1][2:8]
            filename = f"{begin}_{end}"
            return filename

    def export_processed_jsonl(
        self,
        author_dict_path: str,
        id_dict_path: str,
        n_items: int = None,
        time_period: tuple = None,
        chunk: int = None,
        default: bool = True,
        custom_keys: list = None,
        custom_filename: str = None,
    ):
        """
        export jsonl file with processed lines with chosen keys
        always return default keys
        optionally return the custom keys
        """
        # generator
        if n_items:
            filename = f"{n_items}lines"
            gen = self.get_generator(n_items=n_items)
        elif time_period:
            if chunk:
                gen = self.get_generator(time_period=time_period)
            else:
                filename = self.get_filenames(time_period)
                gen = self.get_generator(time_period=time_period)
        else:
            filename = "all"
            gen = self.get_generator()

        # output path
        if chunk:
            time_periods = self.get_periods(time_period, chunk)
            filenames = self.get_filenames(time_periods)
            index = 0
        else:
            if custom_filename:
                output_path = (
                    Path(self.output_directory)
                    / f"processed_{filename}_{custom_filename}.jsonl"
                )
            else:
                output_path = (
                    Path(self.output_directory) / f"processed_{filename}.jsonl"
                )

        # load dicts
        with open(author_dict_path, "r") as f1:
            author_dict = json.load(f1)
        with open(id_dict_path, "r") as f2:
            id_dict = json.load(f2)

        # process
        for line in tqdm(gen):
            # output path for chunk
            if chunk:
                # time period
                time_period = time_periods[index]
                end_epoch = self.get_epoch(time_period[1])

                # check path change condition
                utc = line.get("created_utc")
                if utc > end_epoch:
                    # change output path
                    index += 1

                # output path
                filename = filenames[index]
                if custom_filename:
                    output_path = (
                        Path(self.output_directory)
                        / f"processed_{filename}_{custom_filename}.jsonl"
                    )
                else:
                    output_path = (
                        Path(self.output_directory) / f"processed_{filename}.jsonl"
                    )

            newline = {}
            if default:
                # Post
                if self.mode == "submissions":
                    # id
                    newline["post_id"] = line.get("id")
                    # author
                    author = line.get("author")
                    newline["author"] = author
                    # total_posts
                    posts = author_dict.get(author)
                    newline["total_posts"] = posts
                    # title
                    newline["title"] = line.get("title")
                    # selftext
                    newline["selftext"] = line.get("selftext")
                    # flair
                    newline["link_flair_text"] = line.get("link_flair_text")
                    # removed_by_category
                    newline["removed_by_category"] = line.get("removed_by_category")
                    # utc
                    utc = line.get("created_utc")
                    newline["created_utc"] = utc
                    # date
                    if utc:
                        newline["date"] = datetime.datetime.fromtimestamp(utc).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                    else:
                        newline["date"] = None
                    # month
                    date = newline["date"]
                    if date:
                        month = date[:7]
                        newline["month"] = month
                    else:
                        newline["month"] = None
                    # score
                    newline["score"] = line.get("score")

                    # upvote_ratio
                    newline["upvote_ratio"] = line.get("upvote_ratio")

                    # num_comments
                    newline["num_comments"] = line.get("num_comments")

                    # num_crossposts
                    newline["num_crossposts"] = line.get("num_crossposts")

                    # subreddit_subscribers
                    newline["subreddit_subscribers"] = line.get("subreddit_subscribers")

                # Comment
                if self.mode == "comments":
                    # id
                    post_id = line.get("link_id")
                    if post_id:
                        newline["post_id"] = post_id.split("_")[1]
                    else:
                        newline["post_id"] = None
                    newline["comment_id"] = line.get("id")
                    # author
                    author = line.get("author")
                    newline["author"] = author
                    # total_comments
                    coms = author_dict.get(author)
                    newline["total_comments"] = coms
                    # author_created_utc
                    u = line.get("author_created_utc")
                    newline["author_created_utc"] = u
                    # author_created_date
                    if u:
                        newline[
                            "author_created_date"
                        ] = datetime.datetime.fromtimestamp(u).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                    else:
                        newline["author_created_date"] = None
                    # body
                    newline["body"] = line.get("body")
                    # flair & post title
                    if post_id:
                        i = newline["post_id"]
                        flair = id_dict["flair"].get(i)
                        title = id_dict["title"].get(i)
                    newline["link_flair_text"] = flair
                    newline["title"] = title
                    # utc
                    utc = line.get("created_utc")
                    newline["created_utc"] = utc
                    # date
                    if utc:
                        newline["date"] = datetime.datetime.fromtimestamp(utc).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                    else:
                        newline["date"] = None
                    # month
                    date = newline["date"]
                    if date:
                        month = date[:7]
                        newline["month"] = month
                    else:
                        newline["month"] = None
                    # score
                    newline["score"] = line.get("score")
                    # no_follow
                    newline["no_follow"] = line.get("no_follow")
                    # collapsed_reason_code
                    newline["collapsed_reason_code"] = line.get("collapsed_reason_code")
                    # controversiality
                    newline["controversiality"] = line.get("controversiality")
                    # banned_by
                    newline["banned_by"] = line.get("banned_by")
                    # is_submitter
                    newline["is_submitter"] = line.get("is_submitter")
            if custom_keys:
                for key in custom_keys:
                    newline[key] = line.get(key)

            # export data
            with output_path.open(mode="a+", encoding="utf-8") as f:
                json_record = json.dumps(newline, ensure_ascii=False)
                f.write(json_record + "\n")

    def export_author_dict(
        self,
        n_items: int = None,
        time_period: tuple = None,
        chunk: int = None,
    ):
        """
        export a dict: {author: total_post/comment_number}

        Allowed combinations:
        - n_items (time_period and chunk are None)
        - time_period (n_items and chunk are None)
        - time_period and chunk (n_items is None)
        - all are None
        """
        # generator
        if n_items:
            filename = f"{n_items}lines"
            gen = self.get_generator(n_items=n_items)
        elif time_period:
            if chunk:
                gen = self.get_generator(time_period=time_period)
            else:
                filename = self.get_filenames(time_period)
                gen = self.get_generator(time_period=time_period)
        else:
            filename = "all"
            gen = self.get_generator()

        # if split into chunks
        if chunk:
            time_periods = self.get_periods(time_period, chunk)
            filenames = self.get_filenames(time_periods)
            author_dict = {}
            index = 0
            for line in tqdm(gen):
                # time period
                time_period = time_periods[index]
                end_epoch = self.get_epoch(time_period[1])
                # filename
                filename = filenames[index]

                # check export condition
                utc = line.get("created_utc")
                if utc > end_epoch:
                    # sort dict by values
                    author_dict = self.sort_dict(author_dict)
                    # export dict
                    output_path = (
                        Path(self.output_directory)
                        / f"author_dict_{self.mode}_{filename}.json"
                    )
                    with output_path.open(mode="a+", encoding="utf-8") as f:
                        json_record = json.dumps(author_dict, ensure_ascii=False)
                        f.write(json_record)
                    # reset
                    author_dict = {}
                    index += 1

                # generate content
                author = line.get("author")
                if author not in author_dict.keys():
                    author_dict[author] = 1
                else:
                    author_dict[author] += 1

        else:
            author_dict = {}
            for line in tqdm(gen):
                author = line.get("author")
                if author not in author_dict.keys():
                    author_dict[author] = 1
                else:
                    author_dict[author] += 1
            author_dict = self.sort_dict(author_dict)
            output_path = (
                Path(self.output_directory) / f"author_dict_{self.mode}_{filename}.json"
            )
            with output_path.open(mode="a+", encoding="utf-8") as f:
                json_record = json.dumps(author_dict, ensure_ascii=False)
                f.write(json_record)

    def export_id_dict(
        self,
        n_items: int = None,
        time_period: tuple = None,
        chunk: int = None,
    ):
        assert self.mode == "submissions", "This function only works with submissions."

        # generator
        if n_items:
            filename = f"{n_items}lines"
            gen = self.get_generator(n_items=n_items)
        elif time_period:
            if chunk:
                gen = self.get_generator(time_period=time_period)
            else:
                filename = self.get_filenames(time_period)
                gen = self.get_generator(time_period=time_period)
        else:
            filename = "all"
            gen = self.get_generator()

        # if split into chunks
        if chunk:
            time_periods = self.get_periods(time_period, chunk)
            filenames = self.get_filenames(time_periods)
            id_dict = {"flair": {}, "title": {}}
            index = 0
            for line in tqdm(gen):
                # time period
                time_period = time_periods[index]
                end_epoch = self.get_epoch(time_period[1])
                # filename
                filename = filenames[index]

                # check export condition
                utc = line.get("created_utc")
                if utc > end_epoch:
                    # export dict
                    output_path = (
                        Path(self.output_directory)
                        / f"id_dict_{self.mode}_{filename}.json"
                    )
                    with output_path.open(mode="a+", encoding="utf-8") as f:
                        json_record = json.dumps(id_dict, ensure_ascii=False)
                        f.write(json_record)
                    # reset
                    id_dict = {"flair": {}, "title": {}}
                    index += 1

                # generate content
                id = line.get("id")
                flair = line.get("link_flair_text")
                title = line.get("title")
                if id:
                    # flair
                    if id not in id_dict["flair"].keys():
                        id_dict["flair"][id] = flair
                    else:
                        raise Exception(
                            f"Post with id '{id}' has appeared twice. There may be duplicated files."
                        )
                    # title
                    if id not in id_dict["title"].keys():
                        id_dict["title"][id] = title
                    else:
                        raise Exception(
                            f"Post with id '{id}' has appeared twice. There may be duplicated files."
                        )

        else:
            id_dict = {"flair": {}, "title": {}}
            for line in tqdm(gen):
                id = line.get("id")
                flair = line.get("link_flair_text")
                title = line.get("title")
                if id:
                    # flair
                    if id not in id_dict["flair"].keys():
                        id_dict["flair"][id] = flair
                    else:
                        raise Exception(
                            f"Post with id '{id}' has appeared twice. There may be duplicated files."
                        )
                    # title
                    if id not in id_dict["title"].keys():
                        id_dict["title"][id] = title
                    else:
                        raise Exception(
                            f"Post with id '{id}' has appeared twice. There may be duplicated files."
                        )
            output_path = (
                Path(self.output_directory) / f"id_dict_{self.mode}_{filename}.json"
            )
            with output_path.open(mode="a+", encoding="utf-8") as f:
                json_record = json.dumps(id_dict, ensure_ascii=False)
                f.write(json_record)

    def export_poster_stats(
        self,
        author_dict_path,
        minpost: int,
        n_items: int = None,
        time_period: tuple = None,
        custom_filename: str = None,
    ):
        """
        export author stats (from raw data) in a dict of dicts:
        {{"author_1": {"author": x,
        "active_period": {start_date: x, end_date: x},
        "active_span": str,
        "flair_ratio": {flair_1: num, flair_2: num, ...},
        "remove_retio":{exist: num, reddit: num, mod: num...},
        "total_posts": x,
        "total_score": x,
        "max_score": {postlink: max_score},
        "total_comments": x,
        "comment_ratio": {falir_1: num, flair_2: num, ...}
        "max_comments": {postlink: max_comments}
        "upvote_ratio": {highest: {postlink: max_ratio but not 1.0}, lowest: {postlink: min_ratio}},
        },
        {"author_2": {...}}
        """

        assert self.mode == "submissions", "This function only works with submissions."

        # generator
        if n_items:
            filename = f"{n_items}lines"
            gen = self.get_generator(n_items=n_items)
        elif time_period:
            filename = self.get_filenames(time_period)
            gen = self.get_generator(time_period=time_period)
        else:
            filename = "all"
            gen = self.get_generator()

        # output path
        if custom_filename:
            output_path = (
                Path(self.output_directory)
                / f"poster_stats_{filename}_(posts>={minpost})_{custom_filename}.json"
            )
        else:
            output_path = (
                Path(self.output_directory)
                / f"poster_stats_{filename}_(posts>={minpost}).json"
            )

        # load dict
        with open(author_dict_path, "r") as f:
            author_dict = json.load(f)
        if minpost > 1:
            authors = [
                author
                for author in author_dict.keys()
                if author_dict[author] >= minpost
            ]
        else:
            authors = [author for author in author_dict.keys()]

        # set up
        result = {}
        for author in authors:
            result[author] = {
                "author": author,
                "active_period": {"start_date": 0, "end_date": 0},
                "active_span": 0,
                "flair_ratio": {"no_flair": 0},
                "remove_ratio": {"exist": 0},
                "total_posts": author_dict[author],
                "total_score": 0,
                "max_score": {},
                "total_comments": 0,
                "comment_ratio": {"no_flair": 0},
                "max_comments": {},
                "upvote_ratio": {"highest": {}, "lowest": {}},
            }

        # generate content

        for line in tqdm(gen):
            if line["author"] in authors:
                author = line.get("author")
                link = line.get("id")

                # dates
                utc = line.get("created_utc")
                if utc:
                    if result[author]["active_period"]["start_date"] == 0:
                        result[author]["active_period"]["start_date"] = utc
                        result[author]["active_period"]["end_date"] = utc
                    else:
                        result[author]["active_period"]["end_date"] = utc
                else:
                    pass

                # flairs
                flair = line.get("link_flair_text")
                if flair:
                    if flair not in result[author]["flair_ratio"].keys():
                        result[author]["flair_ratio"][flair] = 1
                    else:
                        result[author]["flair_ratio"][flair] += 1
                else:
                    result[author]["flair_ratio"]["no_flair"] += 1

                # removes
                remove = line.get("removed_by_category")
                if remove:
                    if remove not in result[author]["remove_ratio"].keys():
                        result[author]["remove_ratio"][remove] = 1
                    else:
                        result[author]["remove_ratio"][remove] += 1
                else:
                    result[author]["remove_ratio"]["exist"] += 1

                # scores
                score = line.get("score") - 1  # filter out the one self upvote
                if score:
                    result[author]["total_score"] += score
                    if result[author]["max_score"] == {}:
                        result[author]["max_score"][link] = score
                    elif score > max(result[author]["max_score"].values()):
                        result[author]["max_score"] = {}
                        result[author]["max_score"][link] = score
                else:
                    pass

                # comments
                c = line.get("num_comments")
                if c:
                    result[author]["total_comments"] += c
                    if result[author]["max_comments"] == {}:
                        result[author]["max_comments"][link] = c
                    elif c > max(result[author]["max_comments"].values()):
                        result[author]["max_comments"] = {}
                        result[author]["max_comments"][link] = c
                    if flair:
                        if flair not in result[author]["comment_ratio"].keys():
                            result[author]["comment_ratio"][flair] = c
                        else:
                            result[author]["comment_ratio"][flair] += c
                    else:
                        result[author]["comment_ratio"]["no_flair"] += c
                else:
                    pass

                # upvote ratio
                r = line.get("upvote_ratio")
                if r is not None and r != 1.0:
                    if (
                        result[author]["upvote_ratio"]["lowest"] == {}
                        and result[author]["upvote_ratio"]["highest"] == {}
                    ):
                        result[author]["upvote_ratio"]["lowest"][link] = r
                        result[author]["upvote_ratio"]["highest"][link] = r
                    elif r <= min(result[author]["upvote_ratio"]["lowest"].values()):
                        result[author]["upvote_ratio"]["lowest"] = {}
                        result[author]["upvote_ratio"]["lowest"][link] = r
                    elif r >= max(result[author]["upvote_ratio"]["highest"].values()):
                        result[author]["upvote_ratio"]["highest"] = {}
                        result[author]["upvote_ratio"]["highest"][link] = r
                    else:
                        pass
                else:
                    pass

        # date convertions
        for line in result.values():
            start = line["active_period"]["start_date"]
            end = line["active_period"]["end_date"]
            line["active_span"] = str(datetime.timedelta(seconds=end - start))
            line["active_period"]["start_date"] = datetime.datetime.fromtimestamp(
                start
            ).strftime("%Y-%m-%d %H:%M:%S")
            line["active_period"]["end_date"] = datetime.datetime.fromtimestamp(
                end
            ).strftime("%Y-%m-%d %H:%M:%S")

        with output_path.open(mode="a+", encoding="utf-8") as f:
            json_record = json.dumps(result, ensure_ascii=False)
            f.write(json_record)

    def export_commenter_stats(
        self,
        author_dict_path,
        mincom: int,
        n_items: int = None,
        time_period: tuple = None,
        chunk: int = None,
        custom_filename: str = None,
    ):
        """
        export author stats (from processed data) in a dict of dicts:
        {{"author_1":
            "author": xxx,
            "author_created_date": x,
            "active_period": {"start_date": x, "end_date": x},
            "active_span": str,
            "flair_ratio": {"no_flair": 0},
            "total_comments": author_dict[author],
            "total_comments_as_submitter": 0,
            "total_score": 0,
            "max_score": {},
            "banned_by": [],
            "controversiality": 0,,
         {"author_2": {...}}
        """
        assert self.mode == "comments", "This function only works with comments."

        # generator
        if n_items:
            filename = f"{n_items}lines"
            gen = self.get_generator(n_items=n_items)
        elif time_period:
            if chunk:
                gen = self.get_generator(time_period=time_period)
            else:
                filename = self.get_filenames(time_period)
                gen = self.get_generator(time_period=time_period)
        else:
            filename = "all"
            gen = self.get_generator()

        # output path
        if chunk:
            time_periods = self.get_periods(time_period, chunk)
            filenames = self.get_filenames(time_periods)
            index = 0
        else:
            if custom_filename:
                output_path = (
                    Path(self.output_directory)
                    / f"commenter_stats_{filename}_(comments>={mincom})_{custom_filename}.json"
                )
            else:
                output_path = (
                    Path(self.output_directory)
                    / f"commenter_stats_{filename}_(comments>={mincom}).json"
                )

        # loda dict
        with open(author_dict_path, "r") as f:
            author_dict = json.load(f)
        if mincom > 1:
            authors = [
                author for author in author_dict.keys() if author_dict[author] >= mincom
            ]
        else:
            authors = [author for author in author_dict.keys()]

        # set up
        result = {}
        for author in authors:
            result[author] = {
                "author": author,
                "author_created_date": 0,
                "active_period": {"start_date": 0, "end_date": 0},
                "active_span": 0,
                "flair_ratio": {"no_flair": 0},
                "total_comments": author_dict[author],
                "total_comments_as_submitter": 0,
                "total_score": 0,
                "max_score": {},
                "banned_by": [],
                "controversiality": 0,
            }
        for line in tqdm(gen):
            if line["author"] in authors:
                author = line.get("author")
                com_id = line.get("comment_id")

                # author birth
                b = line.get("author_created_utc")
                if b:
                    result[author][
                        "author_created_date"
                    ] = datetime.datetime.fromtimestamp(b).strftime("%Y-%m-%d %H:%M:%S")
                else:
                    pass

                # dates
                utc = line.get("created_utc")
                if utc:
                    if result[author]["active_period"]["start_date"] == 0:
                        result[author]["active_period"]["start_date"] = utc
                        result[author]["active_period"]["end_date"] = utc
                    else:
                        result[author]["active_period"]["end_date"] = utc
                else:
                    pass

                # flairs
                flair = line.get("link_flair_text")
                if flair:
                    if flair not in result[author]["flair_ratio"].keys():
                        result[author]["flair_ratio"][flair] = 1
                    else:
                        result[author]["flair_ratio"][flair] += 1
                else:
                    result[author]["flair_ratio"]["no_flair"] += 1

                # comments as submitter
                s = line.get("is_submitter")
                if s is True:
                    result[author]["total_comments_as_submitter"] += 1
                else:
                    pass

                # scores
                score = line.get("score") - 1
                if score:
                    result[author]["total_score"] += score
                    if result[author]["max_score"] == {}:
                        result[author]["max_score"][com_id] = score
                    elif score > max(result[author]["max_score"].values()):
                        result[author]["max_score"] = {}
                        result[author]["max_score"][com_id] = score
                else:
                    pass

                # banning
                ban = line.get("banned_by")
                if ban:
                    result[author]["banned_by"].append(ban)

                # controversiality
                c = line.get("controversiality")
                if c != 0:
                    result[author]["controversiality"] += c

            if chunk:
                # output path
                time_period = time_periods[index]
                end_epoch = self.get_epoch(time_period[1])
                filename = filenames[index]
                # export condition
                if utc > end_epoch:
                    # output path
                    output_path = (
                        Path(self.output_directory)
                        / f"commenter_stats_{filename}_(comments>={mincom}).json"
                    )
                    # date conversions
                    for line in result.values():
                        start = line["active_period"]["start_date"]
                        end = line["active_period"]["end_date"]
                        line["active_span"] = str(
                            datetime.timedelta(seconds=end - start)
                        )
                        line["active_period"][
                            "start_date"
                        ] = datetime.datetime.fromtimestamp(start).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                        line["active_period"][
                            "end_date"
                        ] = datetime.datetime.fromtimestamp(end).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                    # export data
                    with output_path.open(mode="a+", encoding="utf-8") as f:
                        json_record = json.dumps(result, ensure_ascii=False)
                        f.write(json_record)
                    # reset
                    result = {}
                    for author in authors:
                        result[author] = {
                            "author": author,
                            "author_created_date": 0,
                            "active_period": {"start_date": 0, "end_date": 0},
                            "active_span": 0,
                            "flair_ratio": {"no_flair": 0},
                            "total_comments": author_dict[author],
                            "total_comments_as_submitter": 0,
                            "total_score": 0,
                            "max_score": {},
                            "banned_by": [],
                            "controversiality": 0,
                        }
                    index += 1

        # date convertions
        for line in result.values():
            start = line["active_period"]["start_date"]
            end = line["active_period"]["end_date"]
            line["active_span"] = str(datetime.timedelta(seconds=end - start))
            line["active_period"]["start_date"] = datetime.datetime.fromtimestamp(
                start
            ).strftime("%Y-%m-%d %H:%M:%S")
            line["active_period"]["end_date"] = datetime.datetime.fromtimestamp(
                end
            ).strftime("%Y-%m-%d %H:%M:%S")

        with output_path.open(mode="a+", encoding="utf-8") as f:
            json_record = json.dumps(result, ensure_ascii=False)
            f.write(json_record)
