# Définitions des termes importants

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
