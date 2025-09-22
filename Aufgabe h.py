#h) Erstellen Sie ein Balkendiagramm, das die Verteilung der Kunden je Bundesland und je
#Geschlecht visualisiert. Die Verteilung soll absteigend sortiert nach Häufigkeit der Kunden je
#Bundesland sein. Nutzen Sie bitte die Methoden im Namespace df.plot.
#h) Show how many customers per state and gender
#Count how many customers are from each state and how many are male/female.

import pandas as pd
import matplotlib.pyplot as plt


df = pd.read_csv("unique_customers.csv")

# Grupp Anzahl je Bundesland + Geschlecht
grouped = df.groupby(["bundesland", "salutation"]).size().unstack(fill_value=0)

#  Gesamtsumme sortieren
grouped["Gesamt"] = grouped.sum(axis=1)
grouped = grouped.sort_values("Gesamt", ascending=False).drop(columns="Gesamt")


grouped.plot(
    kind="bar",
    figsize=(12, 6),
    title="Kundenverteilung je Bundesland und Geschlecht",
    ylabel="Anzahl Kunden"
)

plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("kunden_bundesland_geschlecht.png")
plt.show()
