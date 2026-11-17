#!/usr/bin/env python3
"""reducer.py (Exercice 2) : additionne les compteurs de chaque mot.

Hadoop Streaming trie les lignes par clé avant le reducer :
les lignes d'un même mot arrivent donc consécutivement.
Ce reducer peut aussi servir de combiner (l'addition est associative)."""
import sys

mot_courant = None
total = 0

for ligne in sys.stdin:
    mot, valeur = ligne.rstrip("\n").split("\t", 1)
    try:
        valeur = int(valeur)
    except ValueError:
        continue  # ligne mal formée : on l'ignore
    if mot == mot_courant:
        total += valeur
    else:
        if mot_courant is not None:
            print(f"{mot_courant}\t{total}")
        mot_courant = mot
        total = valeur

# ne pas oublier le dernier mot !
if mot_courant is not None:
    print(f"{mot_courant}\t{total}")
