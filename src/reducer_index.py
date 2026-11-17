#!/usr/bin/env python3
"""reducer_index.py (Exercice 6) : pour chaque mot, liste des fichiers qui le contiennent."""
import sys

mot_courant, fichiers = None, set()

for ligne in sys.stdin:
    mot, fichier = ligne.rstrip("\n").split("\t")
    if mot == mot_courant:
        fichiers.add(fichier)
    else:
        if mot_courant is not None:
            print(f"{mot_courant}\t{','.join(sorted(fichiers))}")
        mot_courant, fichiers = mot, {fichier}

if mot_courant is not None:
    print(f"{mot_courant}\t{','.join(sorted(fichiers))}")
