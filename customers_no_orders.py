from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("CustomersWithNoOrders") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    customers = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("customers.csv")

    orders = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("orders.csv")

    customers_with_no_orders = customers.join(
        orders,
        customers.customer_id == orders.customer_id,
        "left_anti"
    )

    customers_with_no_orders.show()

    spark.stop()


if __name__ == "__main__":
    main()
