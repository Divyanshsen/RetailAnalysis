import pytest
from lib.Utils import get_spark_session

"""
@pytest.fixture
# fixture is a setup part, here it will run this function first and
# give spark variable to the other functions
def spark():
	return get_spark_session("LOCAL")
"""

@pytest.fixture
# fixture is a setup part, here it will run this function first and
# give spark variable to the other functions
# here we have applyed yield as once our execution/work is done,
# we will relese the resouces

def spark():
	"created spark session - fixtures"
	spark_session=get_spark_session("LOCAL")
	yield spark_session
	spark_session.stop()

@pytest.fixture
def expected_state_aggregation(spark):
	"expected_state_aggregation"
	state_schema="state string,count int"
	return spark.read\
	.format("csv")\
	.option("header",True)\
	.schema(state_schema)\
	.load("data/state_aggregation/state_aggregation.csv")


