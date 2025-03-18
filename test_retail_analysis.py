import pytest
#from lib.Utils import get_spark_session
from lib.DataReader import read_customer,read_orders
from lib.DataManipulation import filter_orders_status,join_orders_customer
from lib.ConfReader import get_app_config
"""
@pytest.fixture
# fixture is a setup part, here it will run this function first and
# give spark variable to the other functions
def spark():
	return get_spark_session("LOCAL")
"""
#@pytest.mark.transformation()
@pytest.mark.skip()
def test_read_customers(spark):
	#spark=get_spark_session("LOCAL")
	customer_df=read_customer(spark,"LOCAL")
	customer_records=customer_df.count()
	assert customer_records==12435

#@pytest.mark.transformation()
@pytest.mark.skip()
def test_orders_customers(spark):
	#spark=get_spark_session("LOCAL")
	orders_df=read_orders(spark,"LOCAL")
	orders_records=orders_df.count()
	assert orders_records==68884

#@pytest.mark.transformation()
@pytest.mark.skip()
def test_filter_closed_orders(spark):
	#config=get_app_config("LOCAL")
	orders_df=read_orders(spark,"LOCAL")
	filtered_closed_order_df=filter_orders_status(orders_df,"CLOSED")
	closed_orders_records=filtered_closed_order_df.count()
	assert closed_orders_records==7556

#@pytest.mark.transformation()
@pytest.mark.skip(reason="work in progress")
def test_app_config():
	config=get_app_config("LOCAL")
	customer_path=config["customers.file.path"] # data/customers.csv
	orders_path=config["orders.file.path"] # data/orders.csv
	assert customer_path=="data/customers.csv" and orders_path=="data/orders.csv"


#@pytest.mark.transformation()
@pytest.mark.skip()
def test_check_state_aggregation(spark,expected_state_aggregation):
	orders_df=read_orders(spark,"LOCAL")
	customer_df=read_customer(spark,"LOCAL")
	results_df=join_orders_customer(orders_df,customer_df)
	result=results_df.groupBy("state").count()
	assert result.collect() == expected_state_aggregation.collect()

@pytest.mark.parametrize(
	"state,count",
	[
		("CLOSED",7556),
		("COMPLETE",22900),
		("PENDING_PAYMENT",15030)
	]
)
def test_check_orders_records(spark,state,count):
	orders_df=read_orders(spark,"LOCAL")
	result=orders_df.filter(f"order_status like '{state}'").count()
	assert result==count