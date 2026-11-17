#!/usr/bin/env python3
"""reducer_ventes.py (Exercice 3) : chiffre d'affaires total et nombre de ventes par ville."""
import sys

ville_courante, total, nb = None, 0.0, 0


def emettre(ville, total, nb):
    print(f"{ville}\t{total:.0f}\t{nb}")


for ligne in sys.stdin:
    ville, montant = ligne.rstrip("\n").split("\t")
    montant = float(montant)
    if ville == ville_courante:
        total += montant
        nb += 1
    else:
        if ville_courante is not None:
            emettre(ville_courante, total, nb)
        ville_courante, total, nb = ville, montant, 1

if ville_courante is not None:
    emettre(ville_courante, total, nb)
