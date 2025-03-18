from lib.ConfReader import get_app_config

# creating a customer schema
def getCustomerSchema():
	return "customer_id int,customer_fname string,customer_lname string,username string,password string,address string,city string,state string,pincode int"

# creating a customer dataframe and returning it
def read_customer(spark,env):
	app_config = get_app_config(env)
	customer_file_path = app_config["customers.file.path"]

	customer_df = spark.read\
	.format("csv") \
	.schema(getCustomerSchema()) \
	.option("header","true") \
	.load(customer_file_path)
	return customer_df

#creating orders schema
def getOrdersSchema():
	return "order_id int,order_date date ,order_customer_id int ,order_status string"


def read_orders(spark,env):
	app_config = get_app_config(env)
	orders_file_path = app_config["orders.file.path"]

	orders_df=spark.read\
	.format("csv")\
	.option("header","true")\
	.schema(getOrdersSchema()) \
	.load(orders_file_path)

	return orders_df

def show_aggregation():
	customer_df=read_customer(spark,env)
	orders_df=read_orders(spark,env)

