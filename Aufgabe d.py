#d) Ergänzen Sie das Dataframe durch ein Attribut 'Bundesland', in dem das Bundesland zur PLZ
#gespeichert sein soll. Die Bundesländer entnehmen Sie bitte aus der Datei plz_mapping.txt.


import polars as pl
import os


data_dir = "C:\\Users\\Susha\\Downloads\\webshop"

# Dateien
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

#  Nur Kunden ab 1930 behalten
filtered_customers = customer_data.filter(
    pl.col("birthdate") >= pl.date(1930, 1, 1)
)

#  PLZ-Mapping einlesen
plz_mapping = pl.read_csv(
    plz_file,
    separator="\t",
    encoding="ISO-8859-1",
    truncate_ragged_lines=True,
    schema_overrides={"PLZ": pl.Utf8}
)

#  Spalten umbenennen
plz_mapping = plz_mapping.rename({"PLZ": "postcode", "Bundesland": "bundesland"})

# Join durchführen (linker Join)
kunden_mit_bundesland = filtered_customers.join(
    plz_mapping.select(["postcode", "bundesland"]),  # nur relevante Spalten
    on="postcode",
    how="left"
)

#  Ergebnis anzeigen
print("\nKunden mit Bundesland:")
print(kunden_mit_bundesland.select(["customerNo", "postcode", "birthdate", "bundesland"]))
