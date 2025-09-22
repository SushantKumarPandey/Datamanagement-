#f)
#Ergänzen Sie das Dataframe Artikel um ein weiteres Attribut "Länge", dass die Anzahl
#Buchstaben des Attributs "Beschreibung" beziffert.


import polars as pl
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
# Neue Spalte "Länge" der Beschreibung ergänzen ===
article_data = article_data.with_columns(
    pl.col("description").str.len_chars().alias("Länge")
)

# prüfen ===
print("\nArtikel mit Länge der Beschreibung:")
print(article_data.select(["article_No", "description", "Länge"]))

#speichern
article_data.write_csv("artikel_mit_laenge.csv")
print("\n artikel_mit_laenge.csv wurde erfolgreich gespeichert.")
