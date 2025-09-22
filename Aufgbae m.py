#m) Ermitteln Sie die Anzahl der Bestellpositionen und die Anzahl der retournierten Positionen je
#Bundesland. Die Kundenadresse ist hier als Ort der Bestellung anzunehmen. Bestimmen Sie
#anschließend die Retourenquote (= Anzahl an Retourenpositionen / Anzahl an
#Bestellpositionen#Return rate per state
#For each state, compare how many products were bought vs. how many were returned.
#Then calculate the return rate (returns / sales).


import polars as pl
import os


data_dir = "C:\\Users\\Susha\\Downloads\\webshop"
sales_file = os.path.join(data_dir, "iw_sales.txt")
returns_file = os.path.join(data_dir, "iw_return_line.txt")
customer_file = os.path.join(data_dir, "iw_customer.txt")
plz_file = os.path.join(data_dir, "plz_mapping.txt")


sales = pl.read_csv(sales_file, separator="\t", encoding="ISO-8859-1", ignore_errors=True)
returns = pl.read_csv(returns_file, separator="\t", encoding="ISO-8859-1", ignore_errors=True)
customers = pl.read_csv(customer_file, separator="\t", encoding="ISO-8859-1", ignore_errors=True)
plz_map = pl.read_csv(plz_file, separator="\t", encoding="ISO-8859-1", ignore_errors=True)

# Spalten vorbereiten
customers = customers.select(["customerNo", "postcode"])
plz_map = plz_map.rename({"PLZ": "postcode", "Bundesland": "bundesland"}).select(["postcode", "bundesland"])

#  Kunden mit Bundesland joinen
customers_with_bl = customers.join(plz_map, on="postcode", how="left")

# Bestellungen mit Kunde + Bundesland
sales = sales.select(["customerNo"])
sales = sales.join(customers_with_bl, on="customerNo", how="left")

#  Retouren mit Kunde + Bundesland
returns = returns.select(["customerNo"])
returns = returns.join(customers_with_bl, on="customerNo", how="left")

# Gruppieren: Positionen zählen
bestellungen_pro_bl = sales.group_by("bundesland").agg(
    pl.len().alias("Bestellpositionen")
)

retouren_pro_bl = returns.group_by("bundesland").agg(
    pl.len().alias("Retourenpositionen")
)

# Join + Retourenquote berechnen
summary = bestellungen_pro_bl.join(retouren_pro_bl, on="bundesland", how="left").fill_null(0)

summary = summary.with_columns(
    (pl.col("Retourenpositionen") / pl.col("Bestellpositionen")).alias("Retourenquote")
).sort("Retourenquote", descending=True)

#
print("\nRetourenquote je Bundesland:")
print(summary)

#
summary.write_csv("retourenquote_pro_bundesland.csv")
