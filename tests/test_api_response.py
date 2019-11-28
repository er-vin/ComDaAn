from comdaan import parse_issues
from comdaan import response, Response
import os

PATH_TO_RESOURCES = os.path.join(os.path.dirname(__file__), "resources/")


def test_response_on_issues_return_type():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    a = response(data, "id", "author", "created_at", "discussion")
    assert isinstance(a, Response)


def test_response_on_issues_cols_unanswered():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    a = response(data, "id", "author", "created_at", "discussion")
    assert a.unanswered_issues.columns.tolist() == ["created_at", "unanswered_to_this_date"]


def test_response_on_issues_cols_response_time():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    a = response(data, "id", "author", "created_at", "discussion")
    assert a.response_time.columns.tolist() == [
        "created_at",
        "response_time",
        "response_time_lowess",
        "response_time_formatted",
    ]


def test_response_on_issues_row_count_unanswered():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    a = response(data, "id", "author", "created_at", "discussion")
    assert len(a.unanswered_issues.index) == 147


def test_response_on_issues_row_count_response_time():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    a = response(data, "id", "author", "created_at", "discussion")
    assert len(a.response_time.index) == 135
