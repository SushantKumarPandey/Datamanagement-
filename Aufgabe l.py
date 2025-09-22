#l)
#Was ist die durchschnittliche Anzahl an Bestellpositionen (Zeilennummern) und der
#durchschnittliche Gesamtbetrag einer Bestellung über alle Kunden? (Hinweis: Es kann
#durchaus mehrere Bestellpositionen zu einer Bestellung geben, jede Bestellung ist eindeutig
#durch die Spalte Bestellnummer).Average order size and value
#For all orders: how many items are usually in one order?
#And how much money does one order cost on average?



import polars as pl
import os

data_dir = "C:\\Users\\Susha\\Downloads\\webshop"
sales_file = os.path.join(data_dir, "iw_sales.txt")


sales = pl.read_csv(
    sales_file,
    separator="\t",
    encoding="ISO-8859-1",
    truncate_ragged_lines=True,
    ignore_errors=True
)

# Gruppieren nach Bestellung (orderNo)
bestellungen = sales.group_by("orderNo").agg([
    pl.len().alias("AnzahlPositionen"),
    pl.col("line_amount").sum().alias("Gesamtbetrag")  # Summe der Beträge pro Bestellung
])

#  Durchschnitt berechnen
durchschnittswerte = bestellungen.select([
    pl.col("AnzahlPositionen").mean().alias("ø Positionen pro Bestellung"),
    pl.col("Gesamtbetrag").mean().alias("ø Gesamtbetrag pro Bestellung (€)")
])


print("\nDurchschnittswerte pro Bestellung:")
print(durchschnittswerte)
