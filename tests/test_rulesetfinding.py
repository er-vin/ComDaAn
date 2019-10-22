import stat
import tempfile
import pytest
from shutil import rmtree, copy

from rulesetfinding import *


@pytest.fixture
def tmp_dir():
    return tempfile.mkdtemp()


@pytest.fixture
def tmp_file(tmp_dir):
    tmp_file = tempfile.mkstemp(dir=tmp_dir, suffix=".py")[-1]
    with open(tmp_file, "w") as f:
        f.write("some_python_code = 123456789")
    importlib.invalidate_caches()
    return tmp_file


@pytest.fixture
def name(tmp_file):
    return tmp_file.split("/")[-1]


@pytest.fixture
def parent(tmp_dir):
    return os.path.abspath(os.path.join(tmp_dir, os.pardir))


def cleanup(tmp_resource):
    rmtree(tmp_resource)


def test_find_rulesets_root():
    assert find_rulesets("/") == []


def test_find_rulesets_in_empty_dir(tmp_dir):
    assert find_rulesets(tmp_dir) == []
    cleanup(tmp_dir)


def test_find_rulesets_dirname_path(tmp_dir):
    assert find_rulesets(os.path.dirname(tmp_dir)) == []
    cleanup(tmp_dir)


def test_find_rulesets_relative_path(tmp_dir, tmp_file):
    name = tmp_file.split("/")[-1]
    cur_dir = os.path.join(tmp_dir, "deep/nested/directory/for/some/robustness/testing")
    os.makedirs(cur_dir)
    copy(tmp_file, cur_dir)
    os.chdir(cur_dir)
    assert len(find_rulesets("../../..", name)) == 1
    cleanup(tmp_dir)


def test_find_rulesets_nonexistent_dir(tmp_dir, name):
    assert find_rulesets(os.path.join("/directory/that/doesnt/exist"), name) == []
    cleanup(tmp_dir)


def test_find_ruleset_in_dir_nonexistent_dir(tmp_dir):
    assert find_ruleset_in_dir("/directory/that/doesnt/exist", "doesnt_matter.py", "/directory/that/doesnt") is None
    cleanup(tmp_dir)


def test_len_find_ruleset_in_dir_not_a_py_file(tmp_dir, parent):
    name = tempfile.mkstemp(dir=tmp_dir)[-1]
    assert find_ruleset_in_dir(tmp_dir, name, parent) is None
    cleanup(tmp_dir)


def test_find_ruleset_in_dir_in_empty_dir(tmp_dir, parent):
    assert find_ruleset_in_dir(tmp_dir, "doesnt_matter.py", parent) is None
    cleanup(tmp_dir)


def test_find_ruleset_in_dir_one_file_in_dir(tmp_dir, name, parent):
    mod_name = tmp_dir.split("/")[-1] + "." + name.split(".")[0]
    assert find_ruleset_in_dir(tmp_dir, name, parent).__name__ == mod_name
    cleanup(tmp_dir)


def test_len_find_rulesets_one_file_in_dir(tmp_dir, name):
    assert len(find_rulesets(tmp_dir, name)) == 1
    cleanup(tmp_dir)


def test_find_rulesets_one_file_in_dir(tmp_dir, name):
    mod_name = tmp_dir.split("/")[-1] + "." + name.split(".")[0]
    assert find_rulesets(tmp_dir, name)[0].__name__ == mod_name
    cleanup(tmp_dir)


def test_len_find_rulesets_multiple_py_files_in_dir(tmp_dir, name):
    tempfile.mkstemp(dir=tmp_dir, suffix=".py")[-1]
    tempfile.mkstemp(dir=tmp_dir, suffix=".py")[-1]
    tempfile.mkstemp(dir=tmp_dir, suffix=".py")[-1]
    tempfile.mkstemp(dir=tmp_dir, suffix=".py")[-1]
    assert len(find_rulesets(tmp_dir, name)) == 1
    cleanup(tmp_dir)


def test_find_rulesets_multiple_py_files_in_dir(tmp_dir, name):
    tempfile.mkstemp(dir=tmp_dir, suffix=".py")[-1]
    tempfile.mkstemp(dir=tmp_dir, suffix=".py")[-1]
    tempfile.mkstemp(dir=tmp_dir, suffix=".py")[-1]
    tempfile.mkstemp(dir=tmp_dir, suffix=".py")[-1]
    mod_name = tmp_dir.split("/")[-1] + "." + name.split(".")[0]
    assert find_rulesets(tmp_dir, name)[0].__name__ == mod_name
    cleanup(tmp_dir)


def test_find_rulesets_files_at_different_levels_of_nested_dir(tmp_dir, tmp_file):
    name = tmp_file.split("/")[-1]
    cur_dir = os.path.join(tmp_dir, "deep/nested/directory/for/some/robustness/testing")
    os.makedirs(cur_dir)
    copy(tmp_file, cur_dir)
    assert len(find_rulesets(cur_dir, name)) == 2
    cleanup(tmp_dir)


def test_find_rulesets_multiple_files_in_nested_dir(tmp_dir, tmp_file):
    name = tmp_file.split("/")[-1]
    cur_dir = os.path.join(tmp_dir, "deep/nested/directory/for/some/robustness/testing")
    os.makedirs(cur_dir)
    copy(tmp_file, cur_dir)
    copy(tmp_file, cur_dir.replace("/testing", ""))
    copy(tmp_file, cur_dir.replace("/robustness/testing", ""))
    assert len(find_rulesets(cur_dir, name)) == 4
    cleanup(tmp_dir)


def test_find_rulesets_nested_working_dir(tmp_dir, tmp_file):
    name = tmp_file.split("/")[-1]
    cur_dir = os.path.join(tmp_dir, "deep/nested/directory/for/some/robustness/testing")
    os.makedirs(cur_dir)
    os.chdir(cur_dir)
    assert len(find_rulesets(cur_dir, name)) == 1
    cleanup(tmp_dir)


def test_find_rulesets_different_working_dir(tmp_dir, tmp_file):
    name = tmp_file.split("/")[-1]
    cur_dir = os.path.join(tmp_dir, "deep/nested/directory/for/some/robustness/testing")
    os.makedirs(cur_dir)
    working_dir = os.path.join(tmp_dir, "deep/nested/directory/diverging/from/module/directory")
    os.makedirs(working_dir)
    os.chdir(working_dir)
    assert len(find_rulesets(cur_dir, name)) == 1
    cleanup(tmp_dir)


def test_find_rulesets_bad_dir_permissions(tmp_dir, name):
    permissions = stat.S_IMODE(os.stat(tmp_dir).st_mode)
    dir_with_bad_perm = tmp_dir.replace("/robustness/testing", "")
    os.chmod(dir_with_bad_perm, stat.S_IXUSR)
    assert find_rulesets(tmp_dir, name) == []
    os.chmod(dir_with_bad_perm, permissions)  # Directories with only x rights cannot be removed by rmtree
    cleanup(tmp_dir)


def test_find_ruleset_in_dir_bad_module_permissions_stderr(tmp_dir, tmp_file, parent, capfd):
    name = tmp_file.split("/")[-1]
    permissions = stat.S_IMODE(os.stat(tmp_file).st_mode)
    os.chmod(tmp_file, stat.S_IXUSR)
    find_ruleset_in_dir(tmp_dir, name, parent)
    os.chmod(tmp_file, permissions)
    capture = capfd.readouterr()
    assert capture.err == "Error: An error occurred with : " + tmp_dir.split("/")[-1] + "." + name.split(".")[0] + "\n"
    cleanup(tmp_dir)


def test_find_ruleset_in_dir_bad_module_permissions(tmp_dir, tmp_file, parent):
    name = tmp_file.split("/")[-1]
    os.chmod(tmp_file, stat.S_IXUSR)
    assert find_ruleset_in_dir(tmp_dir, name, parent) is None
    cleanup(tmp_dir)


def test_find_rulesets_one_failure_and_many_successes(tmp_dir, tmp_file):
    name = tmp_file.split("/")[-1]
    cur_dir = os.path.join(tmp_dir, "deep/nested/directory/for/some/robustness/testing")
    os.makedirs(cur_dir)
    copy(tmp_file, cur_dir)
    copy(tmp_file, cur_dir.replace("/testing", ""))
    copy(tmp_file, cur_dir.replace("/robustness/testing", ""))
    os.chmod(tmp_file, stat.S_IXUSR)
    assert len(find_rulesets(cur_dir, name)) == 3
    cleanup(tmp_dir)


def test_find_rulesets_one_failure_and_many_successes_stderr(tmp_dir, tmp_file, capfd):
    name = tmp_file.split("/")[-1]
    cur_dir = os.path.join(tmp_dir, "deep/nested/directory/for/some/robustness/testing")
    os.makedirs(cur_dir)
    copy(tmp_file, cur_dir)
    copy(tmp_file, cur_dir.replace("/testing", ""))
    copy(tmp_file, cur_dir.replace("/robustness/testing", ""))
    os.chmod(tmp_file, stat.S_IXUSR)
    find_rulesets(cur_dir, name)
    capture = capfd.readouterr()
    assert capture.err == "Error: An error occurred with : " + tmp_dir.split("/")[-1] + "." + name.split(".")[0] + "\n"
    cleanup(tmp_dir)


def test_find_rulesets_stderr(tmp_dir, tmp_file, capfd):
    name = tmp_file.split("/")[-1]
    cur_dir = os.path.join(tmp_dir, "deep/nested/directory/for/some/robustness/testing")
    os.makedirs(cur_dir)
    tmp_dir_robustness = cur_dir.replace("/testing", "")
    tmp_dir_some = cur_dir.replace("/robustness/testing", "")
    copy(tmp_file, cur_dir)
    copy(tmp_file, tmp_dir_robustness)
    copy(tmp_file, tmp_dir_some)
    os.chmod(tmp_file, stat.S_IXUSR)
    os.chmod(os.path.join(tmp_dir_robustness, name), stat.S_IXUSR)
    os.chmod(os.path.join(tmp_dir_some, name), stat.S_IXUSR)
    find_rulesets(cur_dir, name)
    capture = capfd.readouterr()
    assert len(capture.err.splitlines()) == 3
    cleanup(tmp_dir)


def test_find_ruleset_in_dir_import(tmp_dir, name, parent):
    assert find_ruleset_in_dir(tmp_dir, name, parent).some_python_code == 123456789
    cleanup(tmp_dir)


def test_find_rulesets_multiple_imports(tmp_dir, tmp_file):
    name = tmp_file.split("/")[-1]
    cur_dir = os.path.join(tmp_dir, "deep/nested/directory/for/some/robustness/testing")
    os.makedirs(cur_dir)
    copy(tmp_file, cur_dir)
    copy(tmp_file, cur_dir.replace("/testing", ""))
    copy(tmp_file, cur_dir.replace("/robustness/testing", ""))
    for mod in find_rulesets(cur_dir, name):
        assert mod.some_python_code == 123456789
    cleanup(tmp_dir)
