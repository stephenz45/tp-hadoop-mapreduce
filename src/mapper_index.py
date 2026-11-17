#!/usr/bin/env python3
"""mapper_index.py (Exercice 6) : émet (mot, nom_du_fichier).
Le nom du fichier lu est dans la variable d'environnement
mapreduce_map_input_file (une tâche Map par fichier)."""
import os
import re
import sys

chemin = os.environ.get("mapreduce_map_input_file", "inconnu")
fichier = os.path.basename(chemin)

for ligne in sys.stdin:
    for mot in re.findall(r"[a-zàâäéèêëîïôöùûüç]+", ligne.strip().lower()):
        print(f"{mot}\t{fichier}")
