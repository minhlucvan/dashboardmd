"""Test that the package can be imported."""


def test_import() -> None:
    """Verify dashboardmd can be imported."""
    import dashboardmd

    assert dashboardmd.__version__ == "0.1.0"
