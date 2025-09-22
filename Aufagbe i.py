
#j)
#Kategorisieren Sie das Attribut Preis in drei "gleichgroße" Kategorien, jede Kategorie soll
#"gleich" viele Artikel abdecken. Gleiche Preise sollen der gleichen Kategorie zugeordnet sein.
#Fügen Sie das kategorisierte Attribut als "Preiskategorie" dem Dataframe "Artikel" hinzu.
#i) Group products by price level
#Put all products into 3 price categories: low, medium, and high —
#each group should have the same number of products.



import polars as pl
import pandas as pd
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

# In Pandas umwandeln
df = article_data.to_pandas()

#  Preiskategorien erstellen (3 gleich große Gruppen)
# Gleiche Preise bleiben in derselben Kategorie → duplicates='drop'
df["Preiskategorie"] = pd.qcut(
    df["unitPrice"],
    q=3,
    labels=["niedrig", "mittel", "hoch"],
    duplicates="drop"
)

# === 3. Ergebnis prüfen
print(df[["unitPrice", "Preiskategorie"]].head(10))

# === 4. Optional speichern
df.to_csv("artikel_mit_preiskategorie.csv", index=False)
print("\n artikel_mit_preiskategorie.csv wurde erfolgreich gespeichert.")
