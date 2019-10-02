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


def find_ruleset_in_dir(dir_path, filename):
    files = []
    for (dirpath, dirnames, filenames) in os.walk(dir_path):
        files.extend(filenames)
        break
    if filename in files:
        spec = importlib.util.spec_from_file_location(
            os.path.basename(dir_path + "_ruleset"), os.path.join(dir_path, filename)
        )
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
            return module
        except:
            print("Error: An error occurred with : " + str(module), file=sys.stderr)


def find_rulesets(path, filename="comdaan_ruleset.py"):
    modules = []
    while os.path.dirname(path) != path:
        mod = find_ruleset_in_dir(path, filename)
        if mod is not None:
            modules.append(mod)
        path = os.path.abspath(os.path.join(path, os.pardir))
    return modules
