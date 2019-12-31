from collections import namedtuple

Activity = namedtuple("Activity", ["dataframe", "authors"])
TeamSize = namedtuple("TeamSize", ["dataframe"])
Network = namedtuple("Network", ["dataframe", "graph"])
Centrality = namedtuple("Centrality", ["centrality", "activity", "size", "name"])
Response = namedtuple("Response", ["unanswered_issues", "response_time"])
