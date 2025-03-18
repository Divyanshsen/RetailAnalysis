from lib.DataReader import read_orders
from lib.DataReader import read_customer


def filter_orders_status(orders_df,status):
	filtered_orders_df=orders_df.filter(f"order_status='{status}'")
	return filtered_orders_df

def count_orders_state(final_df):
	return final_df.groupBy("order_status").count()


def join_orders_customer(orders_df,customer_df):
	joined_df=customer_df.join(orders_df,customer_df.customer_id==orders_df.order_customer_id,"inner")
	return joined_df





