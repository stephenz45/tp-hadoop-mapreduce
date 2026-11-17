#!/usr/bin/env python3
"""mapper_notes.py (Exercice 5) : émet matiere<TAB>note<TAB>1.
Le « 1 » est un compteur : le reducer additionnera séparément les notes
et les compteurs, puis divisera à la fin. Format : etudiant,matiere,note"""
import sys

for ligne in sys.stdin:
    champs = ligne.strip().split(",")
    if len(champs) != 3 or champs[0] == "etudiant":  # ignorer l'en-tête
        continue
    _, matiere, note = champs
    try:
        float(note)
    except ValueError:
        continue
    print(f"{matiere}\t{note}\t1")
