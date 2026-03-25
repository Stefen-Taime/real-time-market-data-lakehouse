from src.utils.notebook_runtime import infer_bundle_target_from_notebook_path


def test_infer_bundle_target_from_notebook_path_for_azure_trial() -> None:
    notebook_path = (
        "/Workspace/Users/user@example.com/"
        ".bundle/real-time-market-data-lakehouse/azure_trial/files/notebooks/10_market_observability_dashboard"
    )

    assert (
        infer_bundle_target_from_notebook_path(
            notebook_path,
            bundle_name="real-time-market-data-lakehouse",
        )
        == "azure_trial"
    )


def test_infer_bundle_target_from_notebook_path_for_prod() -> None:
    notebook_path = (
        "/Workspace/Users/user@example.com/"
        ".bundle/real-time-market-data-lakehouse/prod/files/notebooks/10_market_observability_dashboard"
    )

    assert (
        infer_bundle_target_from_notebook_path(
            notebook_path,
            bundle_name="real-time-market-data-lakehouse",
        )
        == "prod"
    )


def test_infer_bundle_target_from_notebook_path_returns_none_when_not_bundle_path() -> None:
    assert (
        infer_bundle_target_from_notebook_path(
            "/Workspace/Users/user@example.com/Shared/dashboard",
            bundle_name="real-time-market-data-lakehouse",
        )
        is None
    )
