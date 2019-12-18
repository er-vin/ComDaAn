from comdaan import parse_repositories, parse_mail, parse_issues, parse_comments
from comdaan import activity, network, response, teamsize, display, centrality
import os

PATH_TO_RESOURCES = os.path.join(os.path.dirname(__file__), "resources/")


def test_repo_activity_display():
    repo = PATH_TO_RESOURCES + "repo"
    if not os.listdir(repo):
        raise Exception("Empty git submodule. Try: git submodule update --init")
    data = parse_repositories(repo)
    a = activity(data, "id", "author_name", "date")
    display(a)
    assert True


def test_mailinglit_activity_display():
    data = parse_mail(PATH_TO_RESOURCES + "mailinglist.mbox")
    a = activity(data, "message_id", "sender_name", "date")
    display(a)
    assert True


def test_issues_activity_display():
    issues = parse_issues(PATH_TO_RESOURCES + "issues.json")
    comm = parse_comments(issues)
    data = issues.merge(comm, how="outer")
    a = activity(data, "id", "author", "created_at")
    display(a)
    assert True


def test_activity_display_multiple_dfs():
    repo = parse_repositories(PATH_TO_RESOURCES + "repo")
    mail = parse_mail(PATH_TO_RESOURCES + "mailinglist.mbox")
    issues = parse_issues(PATH_TO_RESOURCES + "issues.json")
    comm = parse_comments(issues)
    repo_a = activity(repo, "id", "author_name", "date")
    mail_a = activity(mail, "message_id", "sender_name", "date")
    issues_a = activity(issues.merge(comm, how="outer"), "id", "author", "created_at")
    display([repo_a, mail_a, issues_a])
    assert True


def test_repo_network_display():
    repo = PATH_TO_RESOURCES + "repo"
    if not os.listdir(repo):
        raise Exception("Empty git submodule. Try: git submodule update --init")
    data = parse_repositories(repo)
    a = network(data, "author_name", "files")
    display(a)
    assert True


def test_mailinglit_network_display():
    data = parse_mail(PATH_TO_RESOURCES + "mailinglist.mbox")
    a = network(data, "sender_name", "references", "message_id")
    display(a)
    assert True


def test_issues_network_display():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    a = network(data, "author", "discussion")
    display(a)
    assert True


def test_network_display_multiple_dfs():
    repo = parse_repositories(PATH_TO_RESOURCES + "repo")
    repo_a = network(repo, "author_name", "files")
    mail = parse_mail(PATH_TO_RESOURCES + "mailinglist.mbox")
    mail_a = network(mail, "sender_name", "references", "message_id")
    issues = parse_issues(PATH_TO_RESOURCES + "issues.json")
    issues_a = network(issues, "author", "discussion")
    display([repo_a, mail_a, issues_a])
    assert True


def test_response_display():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    a = response(data, "id", "author", "created_at", "discussion")
    display(a)
    assert True


def test_response_display_multiple_dfs():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    a = response(data, "id", "author", "created_at", "discussion")
    b = response(parse_issues(PATH_TO_RESOURCES + "issues2.json"), "id", "author", "created_at", "discussion")
    display([a, b])
    assert True


def test_teamsize_on_issues_display():
    issues = parse_issues(PATH_TO_RESOURCES + "issues.json")
    comm = parse_comments(issues)
    data = issues.merge(comm, how="outer")
    a = teamsize(data, "id", "author", "created_at")
    display(a)
    assert True


def test_teamsize_on_issues_display_multiple_df():
    issues = parse_issues(PATH_TO_RESOURCES + "issues.json")
    comm = parse_comments(issues)
    data = issues.merge(comm, how="outer")
    a = teamsize(data, "id", "author", "created_at")
    b = teamsize(parse_issues(PATH_TO_RESOURCES + "issues2.json"), "id", "author", "created_at")
    display([a, b])
    assert True


def test_centrality_display():
    repo = PATH_TO_RESOURCES + "repo"
    if not os.listdir(repo):
        raise Exception("Empty git submodule. Try: git submodule update --init")
    data = parse_repositories(repo)
    a = centrality(data, "id", "author_name", "date", "files", name="Alex Merry")
    display(a)
    assert True


def test_centrality_issues():
    b = centrality(
        parse_issues(PATH_TO_RESOURCES + "issues2.json"), "id", "author", "created_at", "discussion", name="asu"
    )
    display(b)
    assert True


def test_centrality_display_multiple_dfs():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    a = centrality(data, "id", "author", "created_at", "discussion", name="mixih")
    b = centrality(
        parse_issues(PATH_TO_RESOURCES + "issues2.json"), "id", "author", "created_at", "discussion", name="asu"
    )
    display([a, b])
    assert True


def test_display_multiple_types():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    a = centrality(data, "id", "author", "created_at", "discussion", name="mixih")
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    b = response(data, "id", "author", "created_at", "discussion")
    data = parse_repositories(PATH_TO_RESOURCES + "repo")
    c = network(data, "author_name", "files")
    display([a, b, c])
    assert True


def test_display_multiple_types_with_multiple_dfs():
    data = parse_issues(PATH_TO_RESOURCES + "issues.json")
    a = centrality(data, "id", "author", "created_at", "discussion", name="mixih")
    b = centrality(
        parse_issues(PATH_TO_RESOURCES + "issues2.json"), "id", "author", "created_at", "discussion", name="asu"
    )
    path = PATH_TO_RESOURCES + "repo"
    if not os.listdir(path):
        raise Exception("Empty git submodule. Try: git submodule update --init")
    repo = parse_repositories(path)
    c = network(repo, "author_name", "files")
    mail = parse_mail(PATH_TO_RESOURCES + "mailinglist.mbox")
    d = network(mail, "sender_name", "references", "message_id")
    issues = parse_issues(PATH_TO_RESOURCES + "issues.json")
    e = network(issues, "author", "discussion")
    issues = parse_issues(PATH_TO_RESOURCES + "issues.json")
    comm = parse_comments(issues)
    data = issues.merge(comm, how="outer")
    f = teamsize(data, "id", "author", "created_at")
    g = teamsize(parse_issues(PATH_TO_RESOURCES + "issues2.json"), "id", "author", "created_at")
    display([a, b, c, d, e, f, g])
    assert True
