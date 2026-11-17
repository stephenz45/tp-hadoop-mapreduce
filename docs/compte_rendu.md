<p align="center">
  <img src="../assets/logo-ufhb.jpg" alt="Université Félix Houphouët-Boigny" width="120">
</p>

# Compte rendu : TP Hadoop sous Ubuntu

**Université Félix Houphouët-Boigny** · Licence 3 Mathématiques-Informatique (BDGL)
**Étudiant :** TRA BI TRA Christian David
**Encadreur :** Docteur BAKAYOKO · **Date :** 9 novembre 2026

---

## Sommaire

0. [Définitions des termes importants](#définitions-des-termes-importants)
1. [Installation et démarrage du cluster](#1-installation-et-démarrage-du-cluster)
2. [Exercice 1 : HDFS](#2-exercice-1--premiers-pas-avec-hdfs)
3. [Exercice 2 : WordCount, combiner et plusieurs reducers](#3-exercice-2--wordcount)
4. [Exercice 3 : chiffre d'affaires par ville](#4-exercice-3--chiffre-daffaires-par-ville)
5. [Exercice 4 : température maximale](#5-exercice-4--température-maximale-par-année)
6. [Exercice 5 : moyenne des notes](#6-exercice-5--moyenne-des-notes-par-matière)
7. [Exercice 6 : index inversé](#7-exercice-6--index-inversé)
8. [Questions de synthèse](#8-questions-de-synthèse)
9. [Annexe : code source commenté](#annexe--code-source-commenté)

---

## Définitions des termes importants

### Cluster et architecture

| Terme | Définition |
|---|---|
| **Cluster** | Ensemble d'ordinateurs indépendants (les nœuds), reliés par un réseau rapide et coordonnés par un logiciel, qui fonctionnent comme un seul système. |
| **Nœud** | Une machine du cluster, avec son propre processeur, sa mémoire et son disque. |
| **Nœud maître / esclave** | Le maître coordonne le cluster et connaît l'emplacement des données. Les esclaves (workers) stockent les données et exécutent les calculs. |
| **Scalabilité verticale (scale-up)** | Rendre une machine plus puissante (plus de CPU, de RAM). Elle atteint vite un plafond. |
| **Scalabilité horizontale (scale-out)** | Ajouter des machines au cluster. C'est le modèle utilisé par Hadoop. |
| **Mode pseudo-distribué** | Tous les démons Hadoop tournent sur une seule machine, chacun dans son propre processus. C'est le mode de ce TP. |
| **Démon** | Processus qui tourne en arrière-plan (par exemple NameNode ou DataNode). |
| **Localité des données** | Principe qui consiste à envoyer le programme (petit) vers les nœuds qui stockent les données (volumineuses), plutôt que l'inverse. |
| **Heartbeat** | Signal de vie envoyé régulièrement par un nœud pour dire qu'il fonctionne. |

### Hadoop et HDFS

| Terme | Définition |
|---|---|
| **Apache Hadoop** | Framework libre, écrit en Java, qui permet de stocker et de traiter de très gros volumes de données sur un cluster de machines ordinaires. |
| **Modules d'Hadoop** | Quatre modules : *Hadoop Common* (bibliothèques partagées), *HDFS* (stockage), *YARN* (ressources et ordonnancement) et *MapReduce* (modèle de calcul et moteur d'exécution). |
| **HDFS** | Hadoop Distributed File System : système de fichiers distribué et tolérant aux pannes. |
| **Bloc** | Fragment d'un fichier HDFS. Taille par défaut : 128 Mo. Un petit fichier n'occupe que sa taille réelle. |
| **Facteur de réplication** | Nombre de copies de chaque bloc (3 par défaut, 1 dans ce TP car il n'y a qu'une machine). |
| **NameNode** | Démon maître de HDFS : il gère l'arborescence et sait sur quels DataNodes se trouve chaque bloc. Il ne stocke aucune donnée utilisateur. |
| **DataNode** | Démon esclave de HDFS : il stocke les blocs et les sert aux clients. |
| **Secondary NameNode** | Fusionne périodiquement le journal des modifications (`edits`) avec l'image du système (`fsimage`). Ce n'est pas un NameNode de secours. |
| **fsimage / edits** | Métadonnées du NameNode : l'image du système de fichiers et le journal de ses modifications. |
| **fsck** | Commande `hdfs fsck` qui vérifie l'état d'un fichier HDFS : blocs, emplacements, santé (HEALTHY). |

### YARN

| Terme | Définition |
|---|---|
| **YARN** | Yet Another Resource Negotiator : gestionnaire des ressources (CPU, mémoire) et ordonnanceur des applications. |
| **ResourceManager** | Démon maître de YARN : il arbitre les ressources du cluster et accepte les applications soumises. |
| **NodeManager** | Démon esclave de YARN : il lance et surveille les conteneurs sur son nœud. |
| **ApplicationMaster** | Un par application : il négocie les ressources et pilote les tâches Map et Reduce de son job. |
| **Conteneur** | Unité d'allocation de YARN (de la mémoire et des cœurs) dans laquelle s'exécute une tâche. |
| **Job / application** | Un traitement soumis au cluster, par exemple un WordCount. Il apparaît dans l'interface YARN (port 8088). |
| **JobHistory Server** | Démon qui conserve l'historique des jobs terminés (port 10020). Sans lui, le client affiche une erreur en fin de job. |

### MapReduce

| Terme | Définition |
|---|---|
| **MapReduce** | Modèle de programmation qui traite de gros volumes de données en parallèle avec deux fonctions écrites par le développeur : map et reduce. |
| **Paire clé/valeur** | Forme sous laquelle circulent toutes les données : `(clé, valeur)`. La clé détermine le regroupement. |
| **Split** | Fragment du fichier d'entrée, en général un par bloc, confié à une tâche Map. |
| **Map (mapper)** | Fonction qui transforme chaque enregistrement d'entrée en paires `(clé, valeur)`. |
| **Combine (combiner)** | Mini-réduction locale à la sortie de chaque Map, pour diminuer le volume transféré. Utilisable seulement si l'opération est associative et commutative. |
| **Partition** | Affectation de chaque clé à un reducer (par défaut : hachage de la clé modulo le nombre de reducers). |
| **Shuffle & Sort** | Transfert des paires vers les reducers, puis tri et regroupement par clé : `(clé, [v1, v2, …])`. |
| **Reduce (reducer)** | Fonction qui agrège la liste des valeurs associées à une même clé. |
| **Opération associative et commutative** | Opération dont le résultat ne dépend ni du regroupement ni de l'ordre des valeurs (somme, maximum). La moyenne n'en est pas une. |
| **Hadoop Streaming** | Outil qui permet d'écrire le mapper et le reducer dans n'importe quel langage (ici Python) lisant `stdin` et écrivant sur `stdout`. |
| **Compteurs (counters)** | Statistiques affichées en fin de job : Map input records, Reduce input groups, Combine output records, etc. |
| **`part-0000X` et `_SUCCESS`** | Fichiers de sortie : un `part-0000X` par reducer, et un fichier vide `_SUCCESS` si le job a réussi. |
| **Index inversé** | Structure qui associe à chaque mot la liste des fichiers qui le contiennent. |

### Les phases d'un job MapReduce

| Phase | Qui | Rôle |
|---|---|---|
| 1. Input / Split | Hadoop | Le fichier d'entrée est découpé en splits (en général un par bloc). Chaque split est confié à une tâche Map. |
| 2. Map | Développeur | Chaque ligne est transformée en paires `(clé, valeur)`. Les tâches Map tournent en parallèle, de préférence là où est le bloc. |
| 3. Combine (facultatif) | Développeur | Mini-réduction locale à la sortie de chaque Map, pour diminuer les données transférées. |
| 4. Partition | Hadoop | Chaque clé est affectée à un reducer (hachage de la clé modulo le nombre de reducers). |
| 5. Shuffle & Sort | Hadoop | Les paires sont transférées vers les reducers, triées et regroupées par clé : `(clé, [v1, v2, …])`. |
| 6. Reduce | Développeur | Pour chaque clé, la fonction reduce agrège la liste des valeurs. |
| 7. Output | Hadoop | Les résultats sont écrits dans HDFS : un fichier `part-0000X` par reducer. |

### Outils utilisés

| Terme | Définition |
|---|---|
| **SSH** | Protocole de connexion sécurisée à une machine. Hadoop l'utilise pour démarrer les démons, d'où la clé sans mot de passe. |
| **`JAVA_HOME`** | Variable d'environnement qui indique où Java est installé (ici OpenJDK 11). |
| **`jps`** | Commande qui liste les processus Java en cours, donc les démons Hadoop actifs. |
| **`hdfs dfs`** | Commandes de manipulation de HDFS (`-mkdir`, `-put`, `-ls`, `-cat`…). |

---

## 1. Installation et démarrage du cluster

Hadoop 3.3.6 a été installé en mode **pseudo-distribué** sur une machine virtuelle VirtualBox (`ubuntu-srv`, Ubuntu 26.04.1 LTS, 2 CPU, 5 096 Mo de RAM, réseau NAT). Des redirections de ports (SSH 2222→22, HDFS 9870, YARN 8088, DataNode 9864) permettent de se connecter depuis Windows.

| Étape | Résultat | Capture |
|---|---|---|
| Paramètres de la VM | 2 CPU, mémoire 5 096 Mo, NAT | ![](../captures/01_vm_processeur.png) ![](../captures/02_vm_memoire.png) |
| Redirection de ports | `ssh`, `hdfs`, `yarn`, `datanode` | ![](../captures/03_vm_redirection_ports.png) |
| Compte `hadoop` | créé avec `adduser` après un premier `Permission denied` | ![](../captures/04_ssh_permission_denied.png) ![](../captures/06_creation_utilisateur_hadoop.png) |
| Java | OpenJDK 11.0.32.1 | ![](../captures/07_java11_version.png) |
| SSH sans mot de passe | `ssh localhost` direct | ![](../captures/08_ssh_localhost_sans_mot_de_passe.png) |
| Téléchargement | `hadoop-3.3.6.tar.gz` (696 Mo) | ![](../captures/09_telechargement_hadoop.png) |
| Variables d'environnement | `hadoop version` affiche 3.3.6 | ![](../captures/10_hadoop_version.png) |
| Configuration | `core-site.xml` et `hdfs-site.xml` | ![](../captures/11_config_core_hdfs_site.png) |
| Formatage HDFS | *successfully formatted* | ![](../captures/12_format_hdfs.png) |
| Démarrage | `jps` : 5 démons + Jps | ![](../captures/13_demarrage_jps.png) |
| Interface NameNode | `localhost:9870`, état *active* | ![](../captures/14_interface_9870_overview.png) ![](../captures/15_interface_9870_capacite.png) |
| Interface YARN | `localhost:8088`, 1 nœud actif | ![](../captures/16_interface_8088_cluster.png) |

La commande `jps` liste bien `NameNode`, `DataNode`, `SecondaryNameNode`, `ResourceManager` et `NodeManager`. Le JobHistory Server a été ajouté plus tard (voir section 3.4).

---

## 2. Exercice 1 : premiers pas avec HDFS

Commandes exécutées : `hdfs dfs -mkdir -p /user/hadoop/tp/input`, `hdfs dfs -put poeme.txt ...`, `hdfs dfs -ls`, `hdfs dfs -cat`, `hdfs fsck ... -files -blocks -locations`.

![](../captures/17_exo1_hdfs_put_ls_cat_fsck.png)
![](../captures/18_exo1_fsck_healthy.png)
![](../captures/19_exo1_blocs_interface_web.png)

**1) Combien de blocs `poeme.txt` occupe-t-il ? Pourquoi ?**
Un seul bloc. Le fichier ne pèse que 85 octets, très en dessous de la taille de bloc par défaut (128 Mo). `fsck` indique `1 block(s)`, `Live_repl=1`, et l'interface web montre *Block 0* de taille 85. Un petit fichier n'occupe pas 128 Mo : le bloc ne consomme que la taille réelle des données.

**2) Retrouver le fichier dans l'interface web.**
Utilities → Browse the file system → `/user/hadoop/tp/input` → `poeme.txt` (capture 19).

**3) Fichier de 1 Go, taille de bloc par défaut ?**
1 Go = 1 024 Mo, soit 1 024 / 128 = **8 blocs**. Avec une réplication de 3, HDFS stocke 8 × 3 = 24 blocs, soit **3 Go** d'espace disque. Dans mon installation, `dfs.replication` vaut 1 (une seule machine) : il n'y aurait que 8 blocs et 1 Go.

---

## 3. Exercice 2 : WordCount

### 3.1 Test local (sans Hadoop)

`cat poeme.txt | ./mapper.py | sort | ./reducer.py`

![](../captures/20_exo2_test_local_wordcount.png)

### 3.2 Exécution sur Hadoop

![](../captures/21_exo2_job_hadoop_log.png)
![](../captures/22_exo2_job_compteurs.png)
![](../captures/23_exo2_resultat_wordcount.png)

Résultat : `apres 1, chat 2, chien 2, court 2, dans 1, dort 1, jardin 1, le 6, sur 1, tapis 1`. Il est identique au test local. Les compteurs sont cohérents avec le schéma du cours :

| Compteur | Valeur | Interprétation |
|---|---|---|
| Map input records | 3 | les 3 lignes du poème |
| Map output records | 18 | 18 mots, chacun émis avec la valeur 1 |
| Reduce input groups | 10 | 10 mots distincts |
| Reduce output records | 10 | une ligne de résultat par mot |
| Combine input/output records | 0 | aucun combiner |
| Shuffled Maps | 2 | Hadoop Streaming a créé 2 splits pour ce petit fichier |

Le job apparaît dans l'interface YARN :

![](../captures/24_yarn_premiere_application.png)

### 3.3 Test sur un vrai texte et effet du combiner

Le roman *Les Misérables* (projet Gutenberg) a été traité deux fois, avec et sans `-combiner "python3 reducer.py"`.

| Compteur | Sans combiner | Avec combiner |
|---|---|---|
| Map input records | 14 399 | 14 399 |
| Map output records | 122 572 | 122 572 |
| Combine input records | 0 | 122 572 |
| Combine output records | 0 | 16 492 |
| Reduce input groups | 12 594 | 12 594 |
| Reduce input records | 122 572 | **16 492** |
| Reduce shuffle bytes | 1 159 001 | **210 374** |
| Spilled records | 245 144 | 32 984 |
| Reduce output records | 12 594 | 12 594 |

![](../captures/25_livre_sans_combiner_compteurs.png)
![](../captures/27_livre_avec_combiner_compteurs.png)

**Analyse.** Sans combiner, les 122 572 paires `(mot, 1)` produites par les Map sont toutes envoyées aux reducers. Avec le combiner, chaque Map fait une mini-réduction locale : il ne reste que 16 492 paires, et le volume transféré passe d'environ 1,16 Mo à 0,21 Mo (réduction d'un facteur 5,5 environ). Le résultat final est **identique**, ce qui confirme que l'addition est associative et commutative.

Top 20 identique dans les deux cas (surtout des mots vides : `de 4472`, `il 3192`, `la 3040`, `et 2949`, `le 2548`, `l 2391`…) :

![](../captures/26_livre_top20_sans_combiner.png)
![](../captures/28_livre_top20_avec_combiner.png)

Page YARN avec les applications terminées :

![](../captures/29_yarn_4_applications.png)

### 3.4 Plusieurs reducers

Option `-D mapreduce.job.reduces=3`. Le job a donné trois fichiers `part-00000`, `part-00001` et `part-00002` (45 057, 46 553 et 46 523 octets, soit 138 133 octets au total, comme avec un seul reducer).

![](../captures/31_trois_reducers_ls.png)
![](../captures/33_trois_reducers_partitions.png)

Chaque reducer a reçu une partie des clés (partitionnement par hachage de la clé modulo 3). Les `head` montrent des mots différents dans chaque fichier, et `grep -cP "^le\t"` sur l'ensemble des `part-*` renvoie **1** : le mot « le » n'apparaît que dans un seul fichier.

**Incident.** Le job s'est terminé avec `FinalApplicationStatus=SUCCEEDED`, puis le client a affiché `Streaming Command Failed!` après dix tentatives de connexion au port 10020.

![](../captures/30_trois_reducers_erreur_historyserver.png)

Cause : le **JobHistory Server** n'était pas démarré, le client ne pouvait donc pas lire le bilan final. Le résultat n'était pas affecté. Correction : `mapred --daemon start historyserver`.

![](../captures/32_jps_jobhistoryserver.png)

---

## 4. Exercice 3 : chiffre d'affaires par ville

Le mapper calcule `quantité × prix_unitaire` et émet `ville<TAB>montant`. Le reducer additionne les montants et compte les lignes de chaque ville.

![](../captures/34_exo3_test_local_ventes.png)
![](../captures/35_exo3_job_ventes_compteurs.png)
![](../captures/36_exo3_resultat_ventes.png)

| Ville | CA total | Nombre de ventes |
|---|---|---|
| Abidjan | 910 000 | 3 |
| Bouake | 445 000 | 2 |
| Korhogo | 40 000 | 1 |
| San-Pedro | 260 000 | 2 |
| Yamoussoukro | 540 000 | 2 |

Compteurs : Reduce input records = 10 (les 10 ventes), Reduce input groups = 5 (les 5 villes), Reduce output records = 5. Le résultat sur Hadoop est identique au test local.

---

## 5. Exercice 4 : température maximale par année

Le mapper émet `annee<TAB>temperature`. Le reducer **garde le maximum** au lieu de faire une somme.

![](../captures/37_exo4_test_local_meteo.png)
![](../captures/38_exo4_resultat_meteo.png)

Résultat : **2024 → 35.0** et **2025 → 36.1**, identique en local et sur Hadoop. Ici un combiner serait utilisable : le maximum est associatif et commutatif, et le maximum des maxima locaux est égal au maximum global.

---

## 6. Exercice 5 : moyenne des notes par matière

![](../captures/39_exo5_test_local_notes.png)
![](../captures/40_exo5_job_notes_compteurs.png)
![](../captures/41_exo5_resultat_notes.png)

Résultat : **Informatique 15.75, Maths 12.25, Physique 11.50**. Compteurs : Map input records = 13 (en-tête + 12 notes), Map output records = 12, Reduce input groups = 3, Reduce output records = 3.

**Question : peut-on utiliser le reducer de moyenne comme combiner ? Justifier.**
Non. La moyenne des moyennes n'est pas la moyenne globale, parce que chaque moyenne partielle est calculée sur un nombre de valeurs différent. Contre-exemple : notes 10, 20 et 5. Moyenne globale = 35/3 ≈ **11,67**. Si un combiner traite d'abord (10, 20) puis (5) : (15 + 5)/2 = **10**. Le résultat est faux.

**Solution proposée.** Le mapper émet `matiere<TAB>note<TAB>1`. Le reducer additionne séparément les notes et les compteurs, puis divise à la fin : `moyenne = Σ notes / Σ compteurs`. La somme et le compteur sont associatifs, donc cette forme reste correcte même si un combiner (qui émettrait des sommes partielles et des compteurs partiels) est ajouté.

---

## 7. Exercice 6 : index inversé

Le mapper lit le nom du fichier courant dans la variable d'environnement `mapreduce_map_input_file` et émet `mot<TAB>fichier`. Le reducer produit pour chaque mot la liste triée des fichiers qui le contiennent.

![](../captures/42_exo6_test_local_index.png)
![](../captures/43_exo6_job_index_compteurs.png)
![](../captures/44_exo6_resultat_index.png)

Compteurs significatifs : **Shuffled Maps = 3** (une tâche Map par fichier, ce qui permet à chaque mapper de connaître son fichier), Reduce input records = 18 (6 + 6 + 6 mots), Reduce input groups = 10, Reduce output records = 10.

Résultat (extrait) : `le → doc1.txt,doc2.txt,doc3.txt`, `chat → doc1.txt,doc3.txt`, `chien → doc2.txt,doc3.txt`, `court → doc2.txt,doc3.txt`, `apres → doc3.txt`.

---

## 8. Questions de synthèse

**1. Définition d'un cluster et trois avantages.**
Un cluster est un ensemble d'ordinateurs indépendants (les nœuds), reliés par un réseau rapide et coordonnés par un logiciel, qui fonctionnent comme un seul système aux yeux de l'utilisateur. Avantages : (1) capacité de stockage de très gros volumes en répartissant les fichiers sur de nombreux disques ; (2) puissance de calcul grâce au traitement en parallèle ; (3) haute disponibilité, car si un nœud tombe en panne les autres prennent le relais. On peut ajouter l'évolutivité et le coût réduit (matériel standard).

**2. Scalabilité verticale et horizontale.**
La scalabilité *verticale* (scale-up) consiste à rendre une machine plus puissante (plus de CPU, de RAM) : elle atteint vite un plafond matériel, coûte cher et laisse un point unique de défaillance. La scalabilité *horizontale* (scale-out) consiste à ajouter des machines au cluster : elle est quasi illimitée, son coût croît de façon linéaire et elle tolère les pannes. **Hadoop exploite la scalabilité horizontale.**

**3. Rôle du NameNode et point sensible.**
Le NameNode gère l'espace de noms HDFS (arborescence, droits) et sait sur quels DataNodes se trouve chaque bloc. Il ne stocke aucune donnée utilisateur. C'est un point sensible : sans ses métadonnées (`fsimage` et `edits`), les blocs restent sur les DataNodes mais on ne peut plus reconstituer les fichiers. Sans configuration de haute disponibilité, il constitue un point unique de défaillance.

**4. Le Secondary NameNode est-il une copie de secours ?**
Non. Il fusionne périodiquement le journal des modifications (`edits`) avec l'image du système (`fsimage`) pour que le journal ne grossisse pas indéfiniment (opération de *checkpoint*). Il ne prend pas le relais du NameNode en cas de panne.

**5. Localité des données.**
C'est le principe qui consiste à envoyer le programme (petit) vers les nœuds qui stockent les données (volumineuses), plutôt que de déplacer les données vers le calcul. Elle est importante car le réseau est le facteur limitant : lire un bloc localement évite un transfert coûteux et améliore le passage à l'échelle.

**6. Phases d'un job MapReduce dans l'ordre.**
1. Input / Split (Hadoop) → 2. **Map** (développeur) → 3. **Combine**, facultatif (développeur) → 4. Partition (Hadoop) → 5. Shuffle & Sort (Hadoop) → 6. **Reduce** (développeur) → 7. Output (Hadoop). Le développeur écrit les phases Map, Combine (s'il en utilise un) et Reduce.

**7. Pourquoi le reducer Python peut-il supposer que les lignes d'une même clé sont consécutives ?**
Parce que Hadoop effectue la phase de Shuffle & Sort avant le Reduce : les paires sont triées et regroupées par clé. Le reducer n'a donc qu'à comparer la clé courante à la précédente pour détecter un changement de groupe (d'où la variable `mot_courant`). Sans ce tri, il faudrait garder tous les compteurs en mémoire.

**8. Quand un combiner est-il utilisable ? Exemple où il ne l'est pas.**
Un combiner est utilisable lorsque l'opération est associative et commutative, et que la sortie du combiner a le même format que l'entrée du reducer : somme (WordCount), maximum (exercice 4). Dans ma mesure, il a fait passer les paires envoyées aux reducers de 122 572 à 16 492. Il n'est pas utilisable directement pour une moyenne (exercice 5) : la moyenne des moyennes n'est pas la moyenne globale.

**9. Panne d'un nœud pendant une tâche Map.**
Le ResourceManager et l'ApplicationMaster détectent l'échec (absence de heartbeat ou erreur de la tâche) et relancent automatiquement la tâche sur un autre nœud. Les données sont lues depuis une autre réplique du bloc HDFS : aucun résultat n'est perdu et le développeur n'a rien à gérer.

**10. Deux limites de MapReduce et un outil de l'écosystème.**
(1) Il est lourd pour les traitements itératifs, car chaque job écrit son résultat sur disque. (2) Sa latence est élevée, ce qui le rend inadapté au temps réel. **Apache Spark** répond à ces deux limites grâce au calcul en mémoire. Pour exprimer des traitements plus simplement, **Hive** permet d'utiliser du SQL.

---

## Conclusion

Ce TP m'a permis d'installer un cluster Hadoop en mode pseudo-distribué, de manipuler HDFS et de suivre l'exécution des jobs dans les interfaces 9870 et 8088. Les six exercices ont été validés : chaque job Hadoop donne le même résultat que sa simulation locale `cat | mapper | sort | reducer`. La mesure du combiner sur *Les Misérables* (122 572 → 16 492 paires) illustre concrètement l'intérêt de réduire les données avant le shuffle. L'exercice sur la moyenne rappelle que le choix de la clé et de la valeur émise détermine ce que l'on peut ou non agréger localement.

---

## Annexe : code source commenté

### `src/mapper.py`

```python
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
```

### `src/reducer.py`

```python
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
```

### `src/mapper_ventes.py`

```python
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
```

### `src/reducer_ventes.py`

```python
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
```

### `src/mapper_meteo.py`

```python
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
```

### `src/reducer_meteo.py`

```python
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
```

### `src/mapper_notes.py`

```python
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
```

### `src/reducer_notes.py`

```python
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
```

### `src/mapper_index.py`

```python
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
```

### `src/reducer_index.py`

```python
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
```
