import importlib


central = importlib.import_module("v6-cohort-builder-py.central")
federated = importlib.import_module("v6-cohort-builder-py.federated")
package = importlib.import_module("v6-cohort-builder-py")


class FakeOrganizations:
    def list(self):
        return [{"id": 1}, {"id": 2}]


class FakeTasks:
    def __init__(self):
        self.created = []

    def create(self, **kwargs):
        self.created.append(kwargs)
        return {"id": 99}


class FakeClient:
    def __init__(self, results):
        self.organization = FakeOrganizations()
        self.task = FakeTasks()
        self._results = results

    def wait_for_results(self, task_id):
        assert task_id == 99
        return self._results


def test_package_root_exposes_central_function_for_vantage6_wrapper():
    assert package.central_function is central.central_function


def test_package_root_exposes_generate_cohort_count_for_vantage6_wrapper():
    assert package.generate_cohort_count is federated.generate_cohort_count


def test_central_counts_the_cohort_at_every_organization():
    client = FakeClient(
        [
            {"organization_id": 1, "status": "success", "cohort_id": 55, "count": 7},
            {"organization_id": 2, "status": "success", "cohort_id": 55, "count": 6},
        ]
    )

    result = central._run_central_function(client)

    assert len(client.task.created) == 1
    created = client.task.created[0]
    assert created["method"] == "cohort_count"
    assert created["organizations"] == [1, 2]
    assert created.get("arguments", {}) == {}
    assert result["cohort_id"] == 55
    assert result["total_count"] == 13
    assert result["errors"] == []


def test_central_asks_only_the_organizations_to_include():
    client = FakeClient([{"organization_id": 2, "status": "success", "count": 6}])

    result = central._run_central_function(client, organizations_to_include=[2])

    assert client.task.created[0]["organizations"] == [2]
    assert result["total_count"] == 6


def test_central_rejects_organizations_outside_the_collaboration():
    client = FakeClient([])

    result = central._run_central_function(client, organizations_to_include=[1, 3])

    assert client.task.created == []
    assert result["status"] == "error"
    assert "3" in result["message"]


def test_central_gives_a_range_when_a_node_withholds_its_count():
    client = FakeClient(
        [
            {"organization_id": 1, "status": "success", "count": 7},
            {
                "organization_id": 2, "status": "success", "count": None,
                "below_minimum": True, "minimum_count": 5,
            },
        ]
    )

    result = central._run_central_function(client)

    # Organization 2 holds 1 to 4 subjects: the total is 8 to 11.
    assert result["total_count"] is None
    assert result["total_range"] == [8, 11]
    assert result["nodes"][1] == {
        "organization_id": 2, "status": "success", "count": None,
        "below_minimum": True, "minimum_count": 5,
    }


def test_central_returns_partial_results_with_errors():
    client = FakeClient(
        [
            {"organization_id": 1, "status": "success", "count": 7},
            {"organization_id": 2, "status": "error", "message": "conflict"},
        ]
    )

    result = central._run_central_function(client)

    assert result["nodes"] == [{"organization_id": 1, "status": "success", "count": 7}]
    assert result["errors"] == [
        {"organization_id": 2, "status": "error", "message": "conflict"}
    ]
    assert result["total_count"] is None
    assert result["total_range"] is None


def test_central_explains_why_hq_refused_the_subtasks():
    client = FakeClient([])
    client.task.create = lambda **kwargs: {
        "msg": "The dataframe you selected is not present for the following organizations: 3."
    }

    result = central._run_central_function(client)

    assert result["status"] == "error"
    assert "not present for the following organizations: 3" in result["message"]
