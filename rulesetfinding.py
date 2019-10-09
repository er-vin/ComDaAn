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

import importlib
import os
import sys


def find_ruleset_in_dir(dir_path, filename, parent):
    files = []
    for (dirpath, dirnames, filenames) in os.walk(dir_path):
        files.extend(filenames)
        break
    if filename in files:
        sys.path.insert(0, parent)
        module_name = os.path.basename(dir_path) + "." + filename.split(".")[0]
        try:
            return importlib.import_module(module_name)

        except:
            print("Error: An error occurred with : " + module_name, file=sys.stderr)


def find_rulesets(path, filename="comdaan_ruleset.py"):
    modules = []
    while os.path.dirname(path) != path:
        parent = os.path.abspath(os.path.join(path, os.pardir))
        mod = find_ruleset_in_dir(path, filename, parent)
        if mod is not None:
            modules.append(mod)
        path = parent
    return modules
