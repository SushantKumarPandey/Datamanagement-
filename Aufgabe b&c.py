#b) Geben Sie die 5 jüngsten Kunden aus.
#c) Entfernen Sie alle Kunden aus "Kunde", die vor 1930 geboren sind.

import polars as pl
import os


data_dir = "C:\\Users\\Susha\\Downloads\\webshop"


customer_file = os.path.join(data_dir, "iw_customer.txt")


schema_overrides = {
    "postcode": pl.Utf8
}


try:
    # Daten einlesen und Datentypen festlegen
    customer_data = pl.read_csv(
        customer_file,
        separator="\t",
        encoding="ISO-8859-1",
        truncate_ragged_lines=True,
        schema_overrides=schema_overrides
    )
    print("Kundendaten erfolgreich geladen!")
except Exception as e:
    print(f"Fehler beim Laden der Kundendaten: {e}")

#  Geburtsdatum als Datum formatieren
try:
    # Geburtsdatum konvertieren
    customer_data = customer_data.with_columns(
        pl.col("birthdate").str.strptime(pl.Date, "%Y-%m-%d %H:%M:%S.%3f", strict=False)
    )
    print("Geburtsdatum erfolgreich formatiert!")
except Exception as e:
    print(f"Fehler bei der Datumsumwandlung: {e}")

#  Jüngste Kunden ausgeben
try:
    # Sortieren und Top 5 ausgeben
    youngest_customers = customer_data.sort("birthdate", descending=True).head(5)
    print("\nDie 5 jüngsten Kunden sind:")
    print(youngest_customers)
except Exception as e:
    print(f"Fehler bei der Ausgabe der jüngsten Kunden: {e}")


# Kunden vor 1930 entfernen
try:
    filtered_customers = customer_data.filter(
        pl.col("birthdate") >= pl.date(1930, 1, 1)
    )
    print("\nKunden ab Geburtsjahr 1930:")
    print(filtered_customers)
except Exception as e:
    print(f"Fehler beim Filtern der Kunden: {e}")