#k)Gibt es Produktgruppen, die bei den Kunden zu erhöhten Mahnstufen führen? Zeigen Sie die
#durchschnittliche Mahnstufe je Produktgruppe über die gekauften Artikel, Retouren sollen nicht
#berücksichtigt werden. Geben Sie die 10 Produktgruppen mit den höchsten durchschnittlichen
#Mahnstufen aus.See which product groups cause payment problems
#For each product group, calculate the average payment delay (credit issues).
#Which ones have the highest average?
#
#


import polars as pl
import os


data_dir = "C:\\Users\\Susha\\Downloads\\webshop"
sales_file = os.path.join(data_dir, "iw_sales.txt")
article_file = os.path.join(data_dir, "iw_article.txt")
customer_file = os.path.join(data_dir, "iw_customer.txt")


sales = pl.read_csv(sales_file, separator="\t", encoding="ISO-8859-1", ignore_errors=True)
articles = pl.read_csv(article_file, separator="\t", encoding="ISO-8859-1", ignore_errors=True)
customers = pl.read_csv(customer_file, separator="\t", encoding="ISO-8859-1", ignore_errors=True)

# arbeitung: Spalten einheitlich formatieren
sales = sales.with_columns([
    pl.col("IWAN").cast(pl.Utf8).str.strip_chars().str.zfill(13),
    pl.col("customerNo").cast(pl.Utf8)
]).select(["IWAN", "customerNo"])

articles = articles.with_columns([
    pl.col("article_No").cast(pl.Utf8).str.strip_chars().str.zfill(13),
    pl.col("productGroup").cast(pl.Utf8)
]).select(["article_No", "productGroup"])

customers = customers.with_columns([
    pl.col("customerNo").cast(pl.Utf8),
    pl.col("credit").cast(pl.Float64)
]).select(["customerNo", "credit"])

# joins
sales_articles = sales.join(articles, left_on="IWAN", right_on="article_No", how="inner")
full_data = sales_articles.join(customers, on="customerNo", how="inner")

#  Durchschnittliche "Mahnstufe"  je Produktgruppe
avg_credit = full_data.group_by("productGroup").agg(
    pl.col("credit").mean().alias("øMahnstufe")
).sort("øMahnstufe", descending=True)

#  Top 10 anzeigen
top10 = avg_credit.head(10)
print("\nTop 10 Produktgruppen mit höchsten durchschnittlichen Mahnstufen (credit):")
print(top10)

#  speichern
top10.write_csv("top10_mahnstufen_produktgruppen.csv")