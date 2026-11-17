#!/usr/bin/env python3
"""reducer_meteo.py (Exercice 4) : température maximale par année.
On garde le maximum au lieu de faire une somme."""
import sys

annee_courante, maximum = None, None

for ligne in sys.stdin:
    annee, temperature = ligne.rstrip("\n").split("\t")
    temperature = float(temperature)
    if annee == annee_courante:
        maximum = max(maximum, temperature)
    else:
        if annee_courante is not None:
            print(f"{annee_courante}\t{maximum:.1f}")
        annee_courante, maximum = annee, temperature

if annee_courante is not None:
    print(f"{annee_courante}\t{maximum:.1f}")
