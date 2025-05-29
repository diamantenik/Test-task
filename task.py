from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def get_product_category_pairs(
        products: DataFrame,
        categories: DataFrame,
        product_categories: DataFrame
) -> DataFrame:
    prod_cat = products.join(product_categories, "product_id", how="left")

    result = prod_cat.join(categories, "category_id", how="left") \
        .select(
        col("product_name"),
        col("category_name")
    )

    return result
