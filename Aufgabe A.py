# Das ER-Diagramm des Webshops,
#Information zu den Daten finden Sie weiter unten in der Aufgabenstellung.
#a) Lesen Sie die Daten für die Entitäten Kunde, Buchung und Artikel, Bestellung, Retoure gemäß
# Datenmodells ein, d.h. passen Sie die Namen der Attribute an, reduzieren Sie die Daten
# auf die Attribute des Datenmodells, beachten Sie die Datentypen.

import polars as pl
import os


data_dir = "C:\\Users\\Susha\\Downloads\\webshop"


files = {
    "customer": "iw_customer.txt",
    "sales": "iw_sales.txt",
    "return_line": "iw_return_line.txt",
    "article": "iw_article.txt",
    "return_header": "iw_return_header.txt"
}

#  Lese für alle Dateien
common_params = {
    "separator": "\t",
    "truncate_ragged_lines": True,
    "encoding": "ISO-8859-1",
    "null_values": ["n/a", "unknown", "NULL", ""]
}

# -Korrekturen für problematische Spalten
schema_overrides = {
    "customer": {"postcode": pl.Utf8},
    "article": {"articleOnline": pl.Utf8}
}

# Funktion zum Laden der Daten
def load_data(file_key):
    file_path = os.path.join(data_dir, files[file_key])
    try:
        schema = schema_overrides.get(file_key, None)
        return pl.read_csv(file_path, **common_params, schema_overrides=schema)
    except Exception as e:
        print(f"Fehler beim Laden von {file_key}: {e}")


customer_data = load_data("customer")
sales_data = load_data("sales")
return_line_data = load_data("return_line")
article_data = load_data("article")



# --- Kunde ---
customer_data = customer_data.select([
    "customerNo", "salutation", "firstname", "surname",
    "postcode", "city", "birthdate"
]).rename({
    "customerNo": "kundenID",
    "salutation": "anrede",
    "firstname": "vorname",
    "surname": "nachname",
    "postcode": "plz",
    "city": "ort",
    "birthdate": "geburtsdatum"
})

# --- Bestellung / Buchung ---
sales_data = sales_data.select([
    "orderNo", "customerNo", "line_No", "IWAN", "quantity",
    "vat_amount", "line_amount", "orderDate"
]).rename({
    "orderNo": "bestellNr",
    "customerNo": "kundennummer",
    "line_No": "zeilennummer",
    "IWAN": "artikelnummer",
    "quantity": "anzahl",
    "vat_amount": "mwst_betrag",
    "line_amount": "zeilen_betrag",
    "orderDate": "bestelldatum"
})

# --- Retoure ---
return_line_data = return_line_data.select([
    "returnNo", "customerNo", "IWAN", "productGroup", "vat_line_amount"
]).rename({
    "returnNo": "retourenNr",
    "customerNo": "kundennummer",
    "IWAN": "artikelnummer",
    "productGroup": "produktgruppe",
    "vat_line_amount": "retoure_betrag"
})


# --- Artikel ---
article_data = article_data.select([
    "IWAN", "description", "unitPrice", "productGroup",
    "colorCode", "colorDescription", "size"
]).rename({
    "IWAN": "artikelnummer",
    "description": "bezeichnung",
    "unitPrice": "preis",
    "productGroup": "kategorie",
    "colorCode": "farbcode",
    "colorDescription": "farbname",
    "size": "groesse"
})


print("\nKUNDE:")
print(customer_data.head())
print("\nBUCHUNG:")
print(sales_data.head())
print("\nRETOURE:")
print(return_line_data.head())
print("\nARTIKEL:")
print(article_data.head())
