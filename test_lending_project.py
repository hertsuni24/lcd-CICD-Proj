import pytest
from lib.DataReader import read_customers, read_orders
from lib.DataManipulation import filter_closed_orders,count_orders_state,filter_orders_generic
from lib.ConfigReader import get_app_config


@pytest.mark.skip()
def test_read_customers_df(spark):
    customers_count=read_customers(spark,'LOCAL').count()
    assert customers_count==12435

@pytest.mark.skip()
def test_read_orders_df(spark):
    orders_count=read_orders(spark,'LOCAL').count()
    assert orders_count==68884

@pytest.mark.skip("work in progress")
def test_filter_closed_orders(spark):
    orders=read_orders(spark,'LOCAL')
    final=filter_closed_orders(orders).count()
    assert final==7556


@pytest.mark.skip()
def test_read_app_config():
    config=get_app_config("LOCAL")
    assert config['customer.file.path']=='data/customers.csv'


@pytest.mark.skip()
def test_count_orders_state(spark,expected_results):
    orders_df=read_orders(spark,'LOCAL')
    resultant_count= count_orders_state(orders_df)
    assert resultant_count.collect()==expected_results.collect()

@pytest.mark.parametrize(
        "status, count",
        [("CLOSED",7556),
         ("PENDING_PAYMENT",15030),
         ("COMPLETE",22900)
         ]
)

def test_filter_orders_generic(spark,status,count):
    order_df=read_orders(spark,'LOCAL')
    filtered_count= filter_orders_generic(order_df,status).count()
    assert filtered_count == count