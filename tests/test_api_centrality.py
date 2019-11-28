from comdaan import parse_issues, parse_mail, parse_repositories
from comdaan import centrality, Centrality
import os

PATH_TO_RESOURCES = os.path.join(os.path.dirname(__file__), "resources/")


def test_centrality_on_repository_no_author():
    repo = PATH_TO_RESOURCES + "repo"
    if not os.listdir(repo):
        raise Exception("Empty git submodule. Try: git submodule update --init")
    data = parse_repositories(repo)
    assert isinstance(centrality(data, "id", "author_name", "date", "files"), list)


def test_centrality_return_type():
    repo = PATH_TO_RESOURCES + "repo"
    if not os.listdir(repo):
        raise Exception("Empty git submodule. Try: git submodule update --init")
    data = parse_repositories(repo)
    assert isinstance(centrality(data, "id", "author_name", "date", "files", name="Alex Merry"), Centrality)


def test_centrality_on_repository_activity_df_cols():
    repo = PATH_TO_RESOURCES + "repo"
    if not os.listdir(repo):
        raise Exception("Empty git submodule. Try: git submodule update --init")
    data = parse_repositories(repo)
    a = centrality(data, "id", "author_name", "date", "files", name="Alex Merry")
    assert a.activity.columns.tolist() == ["date", "value"]


def test_centrality_on_repository_centrality_df_cols():
    repo = PATH_TO_RESOURCES + "repo"
    if not os.listdir(repo):
        raise Exception("Empty git submodule. Try: git submodule update --init")
    data = parse_repositories(repo)
    a = centrality(data, "id", "author_name", "date", "files", name="Alex Merry")
    assert a.centrality.columns.tolist() == ["date", "value"]


def test_centrality_on_repository_size_df_cols():
    repo = PATH_TO_RESOURCES + "repo"
    if not os.listdir(repo):
        raise Exception("Empty git submodule. Try: git submodule update --init")
    data = parse_repositories(repo)
    a = centrality(data, "id", "author_name", "date", "files", name="Alex Merry")
    assert a.size.columns.tolist() == ["date", "value"]


def test_centrality_on_repository_centrality_df_and_size_df_row_count():
    repo = PATH_TO_RESOURCES + "repo"
    if not os.listdir(repo):
        raise Exception("Empty git submodule. Try: git submodule update --init")
    data = parse_repositories(repo)
    a = centrality(data, "id", "author_name", "date", "files", name="Alex Merry")
    assert len(a.size.index) == 71 and len(a.centrality.index) == 71


def test_centrality_on_repository_activity_df_row_count():
    repo = PATH_TO_RESOURCES + "repo"
    if not os.listdir(repo):
        raise Exception("Empty git submodule. Try: git submodule update --init")
    data = parse_repositories(repo)
    a = centrality(data, "id", "author_name", "date", "files", name="Alex Merry")
    assert len(a.activity.index) == 48


def test_centrality_on_mailinglist_no_author():
    data = parse_mail(PATH_TO_RESOURCES + "mailinglist.mbox")
    assert isinstance(centrality(data, "message_id", "sender_name", "date", "references", "message_id"), list)


def test_centrality_on_mailinglists_return_type():
    data = parse_mail(PATH_TO_RESOURCES + "mailinglist.mbox")
    assert isinstance(
        centrality(data, "message_id", "sender_name", "date", "references", "message_id", name="Jay Woods"), Centrality
    )  # Might break here because message_id is used as both a source and an id


def test_centrality_on_mailinglist_activity_cols():
    data = parse_mail(PATH_TO_RESOURCES + "mailinglist.mbox")
    a = centrality(data, "message_id", "sender_name", "date", "references", "message_id", name="Jay Woods")
    assert a.activity.columns.tolist() == ["date", "value"]


def test_centrality_on_mailinglist_centrality_cols():
    data = parse_mail(PATH_TO_RESOURCES + "mailinglist.mbox")
    a = centrality(data, "message_id", "sender_name", "date", "references", "message_id", name="Jay Woods")
    assert a.centrality.columns.tolist() == ["date", "value"]


def test_centrality_on_mailinglist_size_cols():
    data = parse_mail(PATH_TO_RESOURCES + "mailinglist.mbox")
    a = centrality(data, "message_id", "sender_name", "date", "references", "message_id", name="Jay Woods")
    assert a.size.columns.tolist() == ["date", "value"]


def test_centrality_on_mailinglist_centrality_df_and_size_df_row_count():
    data = parse_mail(PATH_TO_RESOURCES + "mailinglist.mbox")
    a = centrality(data, "message_id", "sender_name", "date", "references", "message_id", name="Jay Woods")
    assert len(a.size.index) == 1 and len(a.centrality.index) == 1


def test_centrality_on_mailinglist_activity_df_row_count():
    data = parse_mail(PATH_TO_RESOURCES + "mailinglist.mbox")
    a = centrality(data, "message_id", "sender_name", "date", "references", "message_id", name="Jay Woods")
    assert len(a.activity.index) == 3


def test_centrality_on_issues_no_author():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    assert isinstance(centrality(data, "id", "author", "created_at", "discussion"), list)


def test_centrality_on_issues_return_type():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    assert isinstance(centrality(data, "id", "author", "created_at", "discussion", name="mixih"), Centrality)


def test_centrality_on_issues_activity_cols():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    a = centrality(data, "id", "author", "created_at", "discussion", name="mixih")
    assert a.activity.columns.tolist() == ["created_at", "value"]


def test_centrality_on_issues_centrality_cols():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    a = centrality(data, "id", "author", "created_at", "discussion", name="mixih")
    assert a.centrality.columns.tolist() == ["created_at", "value"]


def test_centrality_on_issues_size_cols():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    a = centrality(data, "id", "author", "created_at", "discussion", name="mixih")
    assert a.size.columns.tolist() == ["created_at", "value"]


def test_centrality_on_issues_centrality_df_and_size_df_row_count():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    a = centrality(data, "id", "author", "created_at", "discussion", name="mixih")
    assert len(a.size.index) == 44 and len(a.centrality.index) == 44


def test_centrality_on_issues_activity_df_row_count():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    a = centrality(data, "id", "author", "created_at", "discussion", name="mixih")
    assert len(a.activity.index) == 23


def test_centrality_on_centrality_df_frac():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    large_frac = centrality(data, "id", "author", "created_at", "discussion", name="mixih", frac=0.8)
    low_frac = centrality(data, "id", "author", "created_at", "discussion", name="mixih", frac=0.025)
    assert not large_frac.centrality.equals(low_frac.centrality)


def test_centrality_size_df_frac():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    large_frac = centrality(data, "id", "author", "created_at", "discussion", name="mixih", frac=0.8)
    low_frac = centrality(data, "id", "author", "created_at", "discussion", name="mixih", frac=0.025)
    assert not large_frac.size.equals(low_frac.size)
