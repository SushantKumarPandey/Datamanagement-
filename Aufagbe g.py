#g) Erstellen Sie je ein Balkendiagramm, das die Verteilung des Attributs "Länge" und "Preis" je
#Produktgruppe des Dataframes Artikel zeigt. Nutzen Sie bitte die Methoden im Namespace
#df.plot.

import polars as pl
import matplotlib.pyplot as plt
import os


data_dir = "C:\\Users\\Susha\\Downloads\\webshop"
article_file = os.path.join(data_dir, "iw_article.txt")


article_data = pl.read_csv(
    article_file,
    separator="\t",
    encoding="ISO-8859-1",
    truncate_ragged_lines=True,
    ignore_errors=True
)

# 3. Neue Spalte "Länge" berechnen
article_data = article_data.with_columns(
    pl.col("description").str.len_chars().alias("Länge")
)

# In Pandas konvertieren
df = article_data.to_pandas()

# Gruppierung: Durchschnitt je Produktgruppe
grouped = df.groupby("productGroup").agg({
    "Länge": "mean",
    "unitPrice": "mean"
})

# 20 nach Beschreibungslänge
top20_länge = grouped.sort_values("Länge", ascending=False).head(20)
top20_länge["Länge"].plot(
    kind="bar",
    figsize=(12, 6),
    title="Ø Beschreibungslänge je Produktgruppe (Top 20)",
    ylabel="Zeichen"
)
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("top20_beschreibungslänge.png")
plt.show()


top20_preis = grouped.sort_values("unitPrice", ascending=False).head(20)
top20_preis["unitPrice"].plot(
    kind="bar",
    figsize=(12, 6),
    color="orange",
    title="Ø Preis je Produktgruppe (Top 20)",
    ylabel="Preis in €"
)
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("top20_preis.png")
plt.show()

