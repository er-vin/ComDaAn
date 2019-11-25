from comdaan import parse_issues, parse_mail, parse_repositories
from comdaan import activity, Activity
from pandas import DataFrame
import os

PATH_TO_RESOURCES = os.path.join(os.path.dirname(__file__), "resources/")


def test_parse_repositories_dataframe_output():
    repo = PATH_TO_RESOURCES + "repo"
    if not os.listdir(repo):
        raise Exception("Empty git submodule. Try: git submodule update --init")
    df = parse_repositories(repo)
    assert isinstance(df, type(DataFrame()))


def test_parse_mail_dataframe_output():
    assert isinstance(parse_mail(PATH_TO_RESOURCES + "mailinglist.mbox"), type(DataFrame()))


def test_parse_issues_dataframe_output():
    assert isinstance(parse_issues(PATH_TO_RESOURCES + "issues.json"), type(DataFrame()))


def test_activity_return_type():
    repo = PATH_TO_RESOURCES + "repo"
    if not os.listdir(repo):
        raise Exception("Empty git submodule. Try: git submodule update --init")
    data = parse_repositories(repo)
    assert isinstance(activity(data, "id", "author_name", "date"), Activity)


def test_activity_on_repository_cols():
    repo = PATH_TO_RESOURCES + "repo"
    if not os.listdir(repo):
        raise Exception("Empty git submodule. Try: git submodule update --init")
    data = parse_repositories(repo)
    a = activity(data, "id", "author_name", "date")
    assert a.dataframe.columns.tolist() == ["author_name", "date", "count", "week_name"]


def test_activity_on_repository_row_count():
    repo = PATH_TO_RESOURCES + "repo"
    if not os.listdir(repo):
        raise Exception("Empty git submodule. Try: git submodule update --init")
    data = parse_repositories(repo)
    a = activity(data, "id", "author_name", "date")
    assert len(a.dataframe.index) == 100


def test_activity_on_repository_author_count():
    repo = PATH_TO_RESOURCES + "repo"
    if not os.listdir(repo):
        raise Exception("Empty git submodule. Try: git submodule update --init")
    data = parse_repositories(repo)
    a = activity(data, "id", "author_name", "date")
    assert len(a.authors) == 36


def test_activity_on_mailinglist_cols():
    data = parse_mail(PATH_TO_RESOURCES + "mailinglist.mbox")
    a = activity(data, "message_id", "sender_name", "date")
    assert a.dataframe.columns.tolist() == ["sender_name", "date", "count", "week_name"]


def test_activity_on_mailinglist_row_count():
    data = parse_mail(PATH_TO_RESOURCES + "mailinglist.mbox")
    a = activity(data, "message_id", "sender_name", "date")
    assert len(a.dataframe.index) == 22


def test_activity_on_mailinglist_author_count():
    data = parse_mail(PATH_TO_RESOURCES + "mailinglist.mbox")
    a = activity(data, "message_id", "sender_name", "date")
    assert len(a.authors) == 8


def test_activity_on_issues_with_reporters_cols():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    a = activity(data, "id", "author", "created_at")
    assert a.dataframe.columns.tolist() == ["author", "created_at", "count", "week_name"]


def test_activity_on_issues_reported_row_count():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    a = activity(data, "id", "author", "created_at")
    assert len(a.dataframe.index) == 84


def test_activity_on_issues_reporter_count():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    a = activity(data, "id", "author", "created_at")
    assert len(a.authors) == 23


def test_activity_on_issues_with_commenters_cols():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    a = activity(data, "id", "author", "created_at", actor="commenter")
    assert a.dataframe.columns.tolist() == ["author", "created_at", "count", "week_name"]


def test_activity_on_issue_comments_row_count():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    a = activity(data, "id", "author", "created_at", actor="commenter")
    assert len(a.dataframe.index) == 182


def test_activity_on_issues_commenters_count():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    a = activity(data, "id", "author", "created_at", actor="commenter")
    assert len(a.authors) == 27


def test_activity_on_issues_with_commenters_vs_reporters():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    comm = activity(data, "id", "author", "created_at", actor="commenter")
    rep = activity(data, "id", "author", "created_at", actor="reporter")
    assert not comm.dataframe.equals(rep.dataframe)


def test_activity_on_issues_with_commenters_and_reporters():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    a = activity(data, "id", "author", "created_at", actor=["commenter", "reporter"])
    assert len(a.dataframe.index) == 204  # 204 is the size of the corresponding dataframe.


def test_activity_on_issues_all_actors_count():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    a = activity(data, "id", "author", "created_at", actor=["commenter", "reporter"])
    assert len(a.authors) == 30
