#e) Leider wird in dem Webshop i.d.R. als Gast ohne Kundenkonto bestellt. Daher existieren viele
#Kunden mit nahezu gleichen Stammdaten (Duplikate), aber mit unterschiedlicher KundenNr.
#Alle Datensätze mit gleichem Wert im Attribut 'riskId' sollen als ein eindeutiger Kunde in einem
#neuen DataFrame 'uniqueCustomers' zusammengeführt werden. Als KundenNr. und alle
#anderen Stammdaten sollen die Einträge des ersten Vorkommens übernommen werden. Das
#DataFrame 'uniqueCustomers' soll persistiert und für die Beantwortung der noch folgenden
#Punkte verwendet werden.

import polars as pl
import os


data_dir = "C:\\Users\\Susha\\Downloads\\webshop"
customer_file = os.path.join(data_dir, "iw_customer.txt")
plz_file = os.path.join(data_dir, "plz_mapping.txt")


schema_overrides = {
    "postcode": pl.Utf8
}

customer_data = pl.read_csv(
    customer_file,
    separator="\t",
    encoding="ISO-8859-1",
    truncate_ragged_lines=True,
    schema_overrides=schema_overrides
).with_columns(
    pl.col("birthdate").str.strptime(pl.Date, "%Y-%m-%d %H:%M:%S.%3f", strict=False)
)

# Kunden vor 1930 entfernen ===
filtered_customers = customer_data.filter(
    pl.col("birthdate") >= pl.date(1930, 1, 1)
)

# === 3. PLZ-Mapping laden ===
plz_mapping = pl.read_csv(
    plz_file,
    separator="\t",
    encoding="ISO-8859-1",
    truncate_ragged_lines=True,
    schema_overrides={"PLZ": pl.Utf8}
).rename({
    "PLZ": "postcode",
    "Bundesland": "bundesland"
})

#  Join mit Bundesland ===
kunden_mit_bundesland = filtered_customers.join(
    plz_mapping.select(["postcode", "bundesland"]),
    on="postcode",
    how="left"
)

# Duplikate entfernen: uniqueCustomers erzeugen ===
unique_customers = kunden_mit_bundesland.unique(
    subset=["riskID"],
    keep="first"
)

#
print("\nEindeutige Kunden basierend auf riskID:")
print(unique_customers.select(["customerNo", "riskID", "birthdate", "bundesland"]))

#   Speichern
unique_customers.write_csv("unique_customers.csv")
print("\n unique_customers.csv wurde erfolgreich gespeichert.")
