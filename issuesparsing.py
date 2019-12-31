#
# Copyright 2019 Christelle Zouein <christellezouein@hotmail.com>
#
# The authors license this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import argparse
import json
import os
import pandas

from datetime import datetime
from pytz import utc
from multiprocess import Pool
from functools import reduce

from rulesetfinding import find_rulesets


ISSUE_FIELDS = [
    "id",
    "iid",
    "project_id",
    "title",
    "description",
    "state",
    "created_at",
    "updated_at",
    "closed_at",
    "closed_by",
    "labels",
    "milestone",
    "assignees",
    "author",
    "assignee",
    "user_notes_count",
    "merge_requests_count",
    "upvotes",
    "downvotes",
    "due_date",
    "confidential",
    "discussion_locked",
    "web_url",
    "time_stats",
    "task_completion_status",
    "has_tasks",
    "_links",
    "weight",
    "discussion",
]


class _IssuesParser:
    def __init__(self):
        self.__paths = []
        self.__rulesets = {}

    @staticmethod
    def get_argument_parser() -> argparse.ArgumentParser:
        arg_parser = argparse.ArgumentParser(add_help=False)
        arg_parser.add_argument(
            "paths",
            metavar="paths",
            nargs="+",
            help="Path of an issues JSON file to process or of a " "directory containing issues JSON files.",
        )
        arg_parser.add_argument("-f", "--start", help="Start date")
        arg_parser.add_argument("-u", "--end", help="End date")

        return arg_parser

    def __add_issues_path(self, path):
        if not isinstance(path, str):
            raise ValueError("String expected")

        abs_path = os.path.abspath(os.path.expanduser(path))
        self.__rulesets[abs_path] = find_rulesets(abs_path, "comdaan_issues.py")
        self.__paths.append(abs_path)

    def add_issues_paths(self, paths):
        if isinstance(paths, str):
            self.__add_issues_paths([paths])
        else:
            self.__add_issues_paths(paths)

    def __add_issues_paths(self, paths):
        for path in paths:
            abs_path = os.path.abspath(os.path.expanduser(path))
            if os.path.isdir(abs_path):
                subpaths = list(map(lambda x: os.path.join(abs_path, x), os.listdir(abs_path)))
                for subpath in subpaths:
                    self.add_issues_paths(subpath)
            else:
                if not path.endswith(".json"):
                    continue
                self.__add_issues_path(path)

    def get_issues(self, start_date=None, end_date=None):
        def wrapper(path):
            return self.__create_entries(path, start_date, end_date)

        with Pool() as pool:
            entries = reduce(lambda a, b: a + b, pool.map(wrapper, self.__paths))

        return pandas.DataFrame(entries, columns=ISSUE_FIELDS)

    def __create_entries(self, path, start_date=None, end_date=None):
        with open(path, "r") as f:
            issues = json.load(f)
        issues = list(map(lambda x: self.__preprocess_entry(x), issues))

        start_datetime = None
        if start_date:
            start_datetime = datetime.strptime(start_date, "%Y-%m-%d")

        end_datetime = None
        if end_date:
            end_datetime = datetime.strptime(end_date, "%Y-%m-%d")

        rulesets = self.__rulesets.get(path, [])

        issues = list(filter(lambda x: self.__is_entry_acceptable(x, start_datetime, end_datetime, rulesets), issues))
        issues = list(map(lambda x: self.__postprocess_entry(x, rulesets), issues))
        return issues

    def __preprocess_entry(self, entry):
        entry["author"] = entry["author"]["name"]

        # Merging all comments of multiple threads in the same big list.
        def get_thread_comments(discussion):
            comments = []
            for thread in discussion:
                comments += thread
            return comments

        entry["discussion"] = get_thread_comments(entry["discussion"])
        for comment in entry["discussion"]:
            comment["author"] = comment["author"]["name"]
            comment["created_at"] = datetime.strptime(comment["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ").astimezone(utc)
            comment["updated_at"] = datetime.strptime(comment["updated_at"], "%Y-%m-%dT%H:%M:%S.%fZ").astimezone(utc)

        for date in ["created_at", "updated_at", "closed_at"] if entry["closed_at"] else ["created_at", "updated_at"]:
            entry[date] = datetime.strptime(entry[date], "%Y-%m-%dT%H:%M:%S.%fZ").astimezone(utc)

        return entry

    def __is_entry_acceptable(self, entry, start_datetime, end_datetime, rulesets):
        for ruleset in rulesets:
            if not ruleset.is_entry_acceptable(entry):
                return False

            if start_datetime and entry["created_at"].date() < start_datetime.date():
                return False

            if end_datetime and entry["created_at"].date() > end_datetime.date():
                return False

        return True

    def __postprocess_entry(self, entry, rulesets):
        for ruleset in rulesets:
            ruleset.postprocess_entry(entry)

        return entry
