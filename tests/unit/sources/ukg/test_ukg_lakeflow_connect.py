from databricks.labs.community_connector.sources.ukg.ukg import UkgLakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestUkgConnector(LakeflowConnectTests):
    connector_class = UkgLakeflowConnect
