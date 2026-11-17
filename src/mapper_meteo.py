#!/usr/bin/env python3
"""mapper_meteo.py (Exercice 4) : émet (annee, temperature).
Format d'entrée : annee,station,temperature"""
import sys

for ligne in sys.stdin:
    champs = ligne.strip().split(",")
    if len(champs) != 3:
        continue
    annee, _, temperature = champs
    try:
        float(temperature)
    except ValueError:
        continue  # ligne mal formée
    print(f"{annee}\t{temperature}")
