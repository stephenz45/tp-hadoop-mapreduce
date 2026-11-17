#!/usr/bin/env python3
"""mapper.py (Exercice 2) : émet (mot, 1) pour chaque mot lu sur l'entrée standard."""
import sys
import re

for ligne in sys.stdin:
    ligne = ligne.strip().lower()
    # on ne garde que les lettres (accents compris)
    mots = re.findall(r"[a-zàâäéèêëîïôöùûüç]+", ligne)
    for mot in mots:
        print(f"{mot}\t1")
