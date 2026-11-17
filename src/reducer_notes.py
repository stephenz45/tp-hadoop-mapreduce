#!/usr/bin/env python3
"""reducer_notes.py (Exercice 5) : moyenne des notes par matière.
On additionne séparément les notes et les compteurs, puis on divise.
La moyenne des moyennes n'est PAS la moyenne globale : ce reducer
ne peut donc pas servir de combiner tel quel."""
import sys

matiere_courante, somme, nb = None, 0.0, 0


def emettre(matiere, somme, nb):
    print(f"{matiere}\t{somme / nb:.2f}")


for ligne in sys.stdin:
    matiere, note, compteur = ligne.rstrip("\n").split("\t")
    if matiere == matiere_courante:
        somme += float(note)
        nb += int(compteur)
    else:
        if matiere_courante is not None:
            emettre(matiere_courante, somme, nb)
        matiere_courante, somme, nb = matiere, float(note), int(compteur)

if matiere_courante is not None:
    emettre(matiere_courante, somme, nb)
