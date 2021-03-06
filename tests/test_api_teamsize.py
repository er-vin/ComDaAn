from comdaan import parse_issues, parse_comments, parse_mail, parse_repositories
from comdaan import teamsize, TeamSize
import os

PATH_TO_RESOURCES = os.path.join(os.path.dirname(__file__), "resources/")


def test_teamsize_return_type():
    repo = PATH_TO_RESOURCES + "repo"
    if not os.listdir(repo):
        raise Exception("Empty git submodule. Try: git submodule update --init")
    data = parse_repositories(repo)
    assert isinstance(teamsize(data, "id", "author_name", "date"), TeamSize)


def test_teamsize_on_repository_cols():
    repo = PATH_TO_RESOURCES + "repo"
    if not os.listdir(repo):
        raise Exception("Empty git submodule. Try: git submodule update --init")
    data = parse_repositories(repo)
    a = teamsize(data, "id", "author_name", "date")
    assert a.dataframe.columns.tolist() == [
        "date",
        "entry_count",
        "author_count",
        "entry_count_lowess",
        "author_count_lowess",
    ]


def test_teamsize_on_repository_row_count():
    repo = PATH_TO_RESOURCES + "repo"
    if not os.listdir(repo):
        raise Exception("Empty git submodule. Try: git submodule update --init")
    data = parse_repositories(repo, end_date="2019-08-12")
    a = teamsize(data, "id", "author_name", "date")
    assert len(a.dataframe.index) == 73


def test_teamsize_on_mailinglists_return_type():
    data = parse_mail(PATH_TO_RESOURCES + "mailinglist.mbox")
    assert isinstance(teamsize(data, "message_id", "sender_name", "date"), TeamSize)


def test_teamsize_on_mailinglist_cols():
    data = parse_mail(PATH_TO_RESOURCES + "mailinglist.mbox")
    a = teamsize(data, "message_id", "sender_name", "date")
    assert a.dataframe.columns.tolist() == [
        "date",
        "entry_count",
        "author_count",
        "entry_count_lowess",
        "author_count_lowess",
    ]


def test_teamsize_on_mailinglist_row_count():
    data = parse_mail(PATH_TO_RESOURCES + "mailinglist.mbox")
    a = teamsize(data, "message_id", "sender_name", "date")
    assert len(a.dataframe.index) == 6


def test_teamsize_on_issues_with_reporters_cols():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    a = teamsize(data, "id", "author", "created_at")
    assert a.dataframe.columns.tolist() == [
        "date",
        "entry_count",
        "author_count",
        "entry_count_lowess",
        "author_count_lowess",
    ]


def test_teamsize_on_issues_reported_row_count():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    a = teamsize(data, "id", "author", "created_at")
    assert len(a.dataframe.index) == 54


def test_teamsize_on_issues_with_commenters_cols():
    issues = parse_issues(PATH_TO_RESOURCES + "issues.json")
    data = parse_comments(issues)
    a = teamsize(data, "id", "author", "created_at")
    assert a.dataframe.columns.tolist() == [
        "date",
        "entry_count",
        "author_count",
        "entry_count_lowess",
        "author_count_lowess",
    ]


def test_teamsize_on_issue_comments_row_count():
    issues = parse_issues(PATH_TO_RESOURCES + "issues.json")
    data = parse_comments(issues)
    a = teamsize(data, "id", "author", "created_at")
    assert len(a.dataframe.index) == 71


def test_teamsize_on_issues_with_commenters_vs_reporters():
    issues = parse_issues(PATH_TO_RESOURCES + "issues.json")
    comments = parse_comments(issues)
    comm = teamsize(comments, "id", "author", "created_at")
    rep = teamsize(issues, "id", "author", "created_at")
    assert not comm.dataframe.equals(rep.dataframe)


def test_teamsize_on_issues_with_commenters_and_reporters():
    issues = parse_issues(PATH_TO_RESOURCES + "issues.json")
    comments = parse_comments(issues)
    data = issues.merge(comments, how="outer")
    a = teamsize(data, "id", "author", "created_at")
    assert len(a.dataframe.index) == 79


def test_teamsize_frac():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    large_frac = teamsize(data, "id", "author", "created_at", frac=0.8)
    low_frac = teamsize(data, "id", "author", "created_at", frac=0.025)
    assert not large_frac.dataframe.equals(low_frac.dataframe)
