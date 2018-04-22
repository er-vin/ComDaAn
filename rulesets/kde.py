#
# Copyright 2018 Kevin Ottens <ervin@ipsquad.net>
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

author_map = {
    "Montel Laurent": "Laurent Montel",
    "Ingo Klcker": "Ingo Klöcker",
    "Aaron J. Seigo": "Aaron Seigo"
}


def is_entry_acceptable(entry):
    if "author_email" not in entry:
        return False

    if entry['author_email'] == "scripty@kde.org":
        return False

    return True


def postprocess_entry(entry):
    if entry['author_name'] in author_map:
        entry['author_name'] = author_map[entry['author_name']]
