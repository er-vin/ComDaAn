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

def is_entry_acceptable(entry):
    try:
        if entry['author_email'] == "scripty@kde.org":
            return False
    except:
        return False

    return True

def postprocess_entry(entry):
    if entry['author_name'] == "Montel Laurent":
        entry['author_name'] = "Laurent Montel"

