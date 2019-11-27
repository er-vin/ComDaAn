#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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
import json
import gitlab
import argparse
import subprocess


def get_project_issues(project_ids, instance, token):
    gl = gitlab.Gitlab(instance, private_token=token)
    issues = []
    for project_id in project_ids:
        issue_objects = gl.projects.get(project_id).issues.list(all=True)
        for issue_object in issue_objects:
            issue = vars(issue_object)["_attrs"]
            discussion = issue_object.discussions.list(all=True)
            # A thread is made up of either a comment or a comment and its replies,
            # and thus a discussion is a list of threads and a thread is a list of comments.
            # The other fields of thread.attribute are metadata that aren't of much interest to us.
            issue["discussion"] = list(map(lambda thread: thread.attributes["notes"], discussion))
            issues.append(issue)

    return issues


def clone_project(access_token, instance, project_ids):
    gl = gitlab.Gitlab(instance, private_token=access_token)
    for project_id in project_ids:
        print(project_id)
        project = gl.projects.get(project_id)
        http_url_to_repo = project.http_url_to_repo.replace("https://", "")
        git_url = "https://oauth2:{}@{}".format(access_token, http_url_to_repo)
        subprocess.call(["git", "clone", git_url])


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(add_help=False)
    arg_parser.add_argument("token", metavar="accessToken", help="Personal access token")
    arg_parser.add_argument("ids", metavar="projectID", nargs="+", help="Project ID of desired issues to retrieve")
    arg_parser.add_argument("-g", "--gitlab-instance", help="Instance to retrieve issues from")
    arg_parser.add_argument("-r", "--repository", help="Clone a repository.", action="store_true")
    arg_parser.add_argument("-i", "--issues", help="Fetch issues.", action="store_true")
    arg_parser.add_argument("-d", "--directory", help="Output directory. (Default is the working directory)")
    arg_parser.add_argument("-o", "--output", help="Issues output file name. (Default: 'issues.json')")

    args = arg_parser.parse_args()
    ins = args.gitlab_instance or "https://gitlab.com/"
    if args.directory:
        if os.path.isdir(args.directory):
            os.chdir(args.directory)
        else:
            print("Please choose a valid directory.")
            exit(2)
    if args.issues:
        output_filename = args.output or "issues.json"
        try:
            with open(output_filename, "w") as f:
                try:
                    issues = get_project_issues(args.ids, ins, args.token)
                except Exception as e:
                    print(e)
                    exit(1)

                json.dump(issues, f)
        except PermissionError:
            print("Please choose a directory/file with write permissions.")
            exit(2)

    if args.repository:
        try:
            clone_project(args.token, ins, args.ids)
        except Exception as e:
            print(e)
            exit(1)
