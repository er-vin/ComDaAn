#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#
# Copyright 2018 Kevin Ottens <ervin@ipsquad.net>
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

from argparse import ArgumentParser
import comdaan as cd

if __name__ == "__main__":
    # Fetching arguments from command line
    arg_parser = ArgumentParser(description="A tool for visualizing, week by week, who contributes code")
    arg_parser.add_argument(
        "paths",
        metavar="path",
        nargs="+",
        help="Path of a git repository to process or of a directory containing git repositories",
    )
    arg_parser.add_argument("-f", "--start", help="Start date")
    arg_parser.add_argument("-u", "--end", help="End date")
    arg_parser.add_argument(
        "--palette", choices=["blue4", "magma256"], default="magma256", help="Choose a palette (default is magma256)"
    )
    arg_parser.add_argument("-t", "--title", help="Title")
    arg_parser.add_argument("-o", "--output", default="result.html", help="Output file (default is 'result.html')")
    args = arg_parser.parse_args()

    start_date = args.start
    end_date = args.end

    data = cd.parse_repositories(args.paths, start_date, end_date)
    a = cd.activity(data, "id", "author_name", "date")
    cd.display(a, palette=args.palette, output=args.output)
