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

import os
import pandas
import argparse
import mailbox

from datetime import datetime
from pytz import utc
from email.utils import parseaddr
from email import policy, message_from_binary_file

from rulesetfinding import find_rulesets

MAIL_FIELDS = ["sender_name", "sender_email", "date", "subject", "in_reply_to", "references", "message_id", "body"]


class _MailParser:
    def __init__(self):
        self.__paths = []
        self.__rulesets = {}

    @staticmethod
    def get_argument_parser() -> argparse.ArgumentParser:
        arg_parser = argparse.ArgumentParser(add_help=False)
        arg_parser.add_argument(
            "paths",
            metavar="path",
            nargs="+",
            help="Path of a mail archive to process or of a directory containing mail archives",
        )
        arg_parser.add_argument("-f", "--start", help="Start date")
        arg_parser.add_argument("-u", "--end", help="End date")
        return arg_parser

    def add_archive(self, path):
        if not isinstance(path, str):
            raise ValueError("String expected")

        abs_path = os.path.abspath(os.path.expanduser(path))
        self.__rulesets[abs_path] = find_rulesets(abs_path, "comdaan_mail.py")
        self.__paths.append(abs_path)

    def add_archives(self, paths):
        if isinstance(paths, str):
            self.__add_archives([paths])
        else:
            self.__add_archives(paths)

    def __add_archives(self, paths):
        for path in paths:
            abs_path = os.path.abspath(os.path.expanduser(path))
            if os.path.isdir(abs_path):
                subpaths = list(map(lambda x: os.path.join(abs_path, x), os.listdir(abs_path)))
                for subpath in subpaths:
                    self.add_archives(subpath)
            else:
                if not path.endswith(".mbox"):
                    continue
                self.add_archive(path)

    def get_emails(self, start_date=None, end_date=None):
        entries = []
        for path in self.__paths:
            entries.extend(self.__create_entries(path, start_date, end_date))
        return pandas.DataFrame(entries, columns=MAIL_FIELDS)

    def __create_entries(self, path, start_date=None, end_date=None):
        emails = []

        def get_body(message):
            if message.is_multipart():
                return get_body(next(message.iter_parts()))
            else:
                return message.get_content()

        for msg in mailbox.mbox(path, factory=lambda f: message_from_binary_file(f, policy=policy.default)):
            sender = parseaddr(msg["From"])
            try:
                body = get_body(msg)
            except:
                body = "Unknown encoding. Failed to retrieve message body."

            email_message = {
                "sender_name": sender[0],
                "sender_email": sender[-1],
                "subject": msg["Subject"],
                "date": msg["Date"],
                "message_id": msg["Message-ID"],
                "references": msg["References"],
                "in_reply_to": msg["In-Reply-To"],
                "body": body,
            }
            emails.append(email_message)

        start_datetime = None
        if start_date:
            start_datetime = datetime.strptime(start_date, "%Y-%m-%d")

        end_datetime = None
        if end_date:
            end_datetime = datetime.strptime(end_date, "%Y-%m-%d")

        rulesets = self.__rulesets.get(path, [])

        emails = list(filter(lambda x: self.__is_entry_acceptable(x, start_datetime, end_datetime, rulesets), emails))
        emails = list(map(lambda x: self.__postprocess_entry(x, rulesets), emails))
        return emails

    def __is_entry_acceptable(self, entry, start_datetime, end_datetime, rulesets):
        for ruleset in rulesets:
            if not ruleset.is_entry_acceptable(entry):
                return False

        try:
            entry_datetime = datetime.strptime(entry["date"], "%a, %d %b %Y %H:%M:%S %z").astimezone(utc)

            if start_datetime and entry_datetime.date() < start_datetime.date():
                return False

            if end_datetime and entry_datetime.date() > end_datetime.date():
                return False

            if entry_datetime.date() > datetime.now().date():
                return False
        except:
            return False

        return True

    def __postprocess_entry(self, entry, rulesets):
        if entry["references"]:
            entry["references"] = entry["references"].split(" ")
        else:
            entry["references"] = []

        entry["date"] = datetime.strptime(entry["date"], "%a, %d %b %Y %H:%M:%S %z").astimezone(utc)

        for ruleset in rulesets:
            ruleset.postprocess_entry(entry)

        return entry
