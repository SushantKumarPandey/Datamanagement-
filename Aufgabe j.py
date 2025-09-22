#Gruppieren Sie die Umsätze des Jahres 2010 je Monat und stellen Sie diese in einem
#Balkendiagramm dar. Die Spalte "Bestelldatum" ist für die Feststellung des Bestelldatums zu
#verwenden. Berücksichtigen Sie auch etwaige Retouren von Bestellungen aus dem genannten
#Zeitraum. Jede Bestellposition (Zeilennummer) wird bei den Retouren einzeln unter der gleichen
#RetourenNummer gelistet. Es ist daher durchaus möglich, dass es mehrere Retouren zu einer
#Bestellung gibt.j) Show sales per month (2010)
#Add up sales for each month of 2010.
#Also subtract returns to get the real (net) sales. Show the result in a chart.


import polars as pl
import matplotlib.pyplot as plt
import os


data_dir = "C:\\Users\\Susha\\Downloads\\webshop"
sales_file = os.path.join(data_dir, "iw_sales.txt")
return_file = os.path.join(data_dir, "iw_return_line.txt")


sales = pl.read_csv(
    sales_file,
    separator="\t",
    encoding="ISO-8859-1",
    truncate_ragged_lines=True,
    ignore_errors=True
)

returns = pl.read_csv(
    return_file,
    separator="\t",
    encoding="ISO-8859-1",
    truncate_ragged_lines=True,
    ignore_errors=True
)

# Zunächst Spaltennamen ausgeben, um zu sehen, welche Spalten tatsächlich verfügbar sind
print("Verfügbare Spalten in Verkaufsdaten:", sales.columns)
print("Verfügbare Spalten in Retourendaten:", returns.columns)

# === 3. Datum konvertieren + Umsatzspalte vorbereiten
sales = sales.with_columns([
    pl.col("orderDate").str.strptime(pl.Date, "%Y-%m-%d %H:%M:%S.%3f", strict=False).alias("Datum"),
    pl.col("line_amount").alias("Umsatz")
])

# === 4. Filter: Nur Verkäufe aus dem Jahr 2010
sales_2010 = sales.filter(pl.col("Datum").dt.year() == 2010)

# === 5. Retouren vorbereiten
# Annahme: "line_price" oder "line_amount" könnte statt "amount" vorhanden sein
# Da wir nicht wissen, welche Spalte genau existiert, prüfen wir das dynamisch
if "line_amount" in returns.columns:
    returns = returns.with_columns([
        (pl.col("quantity") * pl.col("line_amount")).alias("RetourenBetrag")
    ])
elif "line_price" in returns.columns:
    returns = returns.with_columns([
        (pl.col("quantity") * pl.col("line_price")).alias("RetourenBetrag")
    ])
elif "price" in returns.columns:
    returns = returns.with_columns([
        (pl.col("quantity") * pl.col("price")).alias("RetourenBetrag")
    ])
else:
    # Falls keine passende Preisspalte gefunden wird, erstellen wir einen Dummy-Wert
    # und geben eine Warnung aus
    print("WARNUNG: Keine Preis-/Betragsspalte in den Retourendaten gefunden!")
    returns = returns.with_columns([
        pl.lit(0).alias("RetourenBetrag")
    ])

# Group by customerNo + IWAN
retouren_summe = returns.group_by(["customerNo", "IWAN"]).agg(
    pl.col("RetourenBetrag").sum().alias("RetourenSumme")
)

# Join on both customerNo + IWAN
sales_joined = sales_2010.join(
    retouren_summe,
    on=["customerNo", "IWAN"],
    how="left"
)


#  Netto-Umsatz berechnen
sales_joined = sales_joined.with_columns(
    (pl.col("Umsatz") - pl.col("RetourenSumme")).alias("NettoUmsatz")
)

#  Monat extrahieren
sales_joined = sales_joined.with_columns(
    pl.col("Datum").dt.month().alias("Monat")
)

# Gruppieren nach Monat und aufsummieren
umsatz_pro_monat = sales_joined.group_by("Monat").agg(
    pl.col("NettoUmsatz").sum().alias("Monatsumsatz")
).sort("Monat")

print("Umsätze pro Monat 2010:")
print(umsatz_pro_monat)

#  Monatsnamen für die Anzeige
monatsnamen = {
    1: "Jan", 2: "Feb", 3: "Mär", 4: "Apr", 5: "Mai", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Okt", 11: "Nov", 12: "Dez"
}

# DataFrame für Plot vorbereiten
df_plot = umsatz_pro_monat.to_pandas()
df_plot["Monatsname"] = df_plot["Monat"].map(monatsnamen)

#
plt.figure(figsize=(12, 6))
bars = plt.bar(df_plot["Monatsname"], df_plot["Monatsumsatz"], color="green")

# Werte über den Balken anzeigen
for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2., height + 5000,
             f'{height:,.0f} €',
             ha='center', va='bottom', rotation=0)

plt.title("Netto-Umsätze pro Monat (2010)", fontsize=15)
plt.ylabel("Umsatz (€)", fontsize=12)
plt.xlabel("Monat", fontsize=12)
plt.grid(axis="y", linestyle="--", alpha=0.7)
plt.xticks(rotation=0)
plt.tight_layout()

#
plt.savefig("umsatz_pro_monat_2010.png", dpi=300)
plt.show()

# Optional: Daten als CSV exportieren
df_plot.to_csv("umsatz_pro_monat_2010.csv", index=False)