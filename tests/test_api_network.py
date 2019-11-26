from comdaan import parse_issues, parse_mail, parse_repositories
from comdaan import network, Network
import os

PATH_TO_RESOURCES = os.path.join(os.path.dirname(__file__), "resources/")


def test_network_return_type():
    repo = PATH_TO_RESOURCES + "repo"
    if not os.listdir(repo):
        raise Exception("Empty git submodule. Try: git submodule update --init")
    data = parse_repositories(repo)
    assert isinstance(network(data, "author_name", "files"), Network)


def test_network_on_repository_cols():
    repo = PATH_TO_RESOURCES + "repo"
    if not os.listdir(repo):
        raise Exception("Empty git submodule. Try: git submodule update --init")
    data = parse_repositories(repo)
    a = network(data, "author_name", "files")
    assert a.dataframe.columns.tolist() == ["centrality"]


def test_network_on_repository_row_count():
    repo = PATH_TO_RESOURCES + "repo"
    if not os.listdir(repo):
        raise Exception("Empty git submodule. Try: git submodule update --init")
    data = parse_repositories(repo)
    a = network(data, "author_name", "files")
    assert len(a.dataframe.index) == 36


def test_network_on_mailinglists_return_type():
    data = parse_mail(PATH_TO_RESOURCES + "mailinglist.mbox")
    assert isinstance(network(data, "sender_name", "references", "message_id"), Network)


def test_network_on_mailinglist_cols():
    data = parse_mail(PATH_TO_RESOURCES + "mailinglist.mbox")
    a = network(data, "sender_name", "references", "message_id")
    assert a.dataframe.columns.tolist() == ["centrality"]


def test_network_on_mailinglist_row_count():
    data = parse_mail(PATH_TO_RESOURCES + "mailinglist.mbox")
    a = network(data, "sender_name", "references", "message_id")
    assert len(a.dataframe.index) == 8


def test_network_on_issues_cols():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    a = network(data, "author", "discussion")
    assert a.dataframe.columns.tolist() == ["centrality"]


def test_network_on_issues_row_count():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    a = network(data, "author", "discussion")
    assert len(a.dataframe.index) == 30
