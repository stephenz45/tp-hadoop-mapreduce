#!/usr/bin/env python3
"""mapper_ventes.py (Exercice 3) : émet (ville, montant) pour chaque vente.
Format d'entrée : date,ville,produit,quantite,prix_unitaire"""
import sys

for ligne in sys.stdin:
    ligne = ligne.strip()
    if not ligne or ligne.startswith("date"):  # ignorer l'en-tête
        continue
    champs = ligne.split(",")
    if len(champs) != 5:
        continue
    _, ville, _, quantite, prix = champs
    try:
        montant = int(quantite) * float(prix)  # montant = quantité x prix unitaire
    except ValueError:
        continue
    print(f"{ville}\t{montant}")
