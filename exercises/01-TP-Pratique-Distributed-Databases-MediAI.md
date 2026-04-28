# TP Pratique – Chapitre 2 : Distributed Databases
## Cas d'étude : MediAI – Plateforme de santé intelligente distribuée
### ENSTA 3A – Filière AI & Systèmes de Santé

---

> **Nom :** Abdeldjabbar  
> **Prénom :** Abba  
> **Date :** 27/04/2026  
> **Note :** ___ / 100

---

## 🌍 Contexte : La plateforme MediAI

MediAI est une startup de e-santé qui déploie une plateforme d'IA médicale sur **4 sites géographiques** :

| Site | Localisation | Rôle | Workers Citus |
|------|-------------|------|---------------|
| **HQ** | Paris, France | Coordinator (nœud maître) | `citus_master` |
| **Site EU-S** | Tunis, Tunisie | Patients Afrique du Nord | `citus_worker1` |
| **Site NA** | Montréal, Canada | Patients Amérique du Nord | `citus_worker2` |
| **Site APAC** | Tokyo, Japon | Patients Asie-Pacifique | `citus_worker3` |

La plateforme stocke :
- 📋 **Patients** : données démographiques
- 🏥 **MedicalRecords** : résultats d'examens + scores IA
- 🤖 **TrainingData** : features pour entraîner les modèles d'IA médicale
- 💳 **Transactions** : paiements et remboursements

---

## ⚙️ Partie 1 – Mise en place du cluster Citus (10 pts)

### 1.1 – Lancement du cluster Docker

Exécutez les commandes suivantes dans votre terminal :

```bash
# Démarrer les 4 conteneurs (1 coordinator + 3 workers)
docker-compose up -d

# Vérifier que les 4 conteneurs sont UP
docker ps

# Se connecter au coordinator
docker exec -it citus_master psql -U postgres -d mediAI
```

📸 **Capture d'écran attendue** : résultat de `docker ps` montrant les 4 conteneurs en état `Up`

> **Collez votre capture ici :**
> 
> ```
> [VOTRE CAPTURE D'ÉCRAN]
> ```

---

### 1.2 – Enregistrement des workers

Une fois connecté au coordinator, enregistrez les 3 workers :

```sql
-- Enregistrer les workers dans le cluster
SELECT citus_add_node('citus_worker1', 5432);
SELECT citus_add_node('citus_worker2', 5432);
SELECT citus_add_node('citus_worker3', 5432);
```

**Question 1.2.a** : Quelle est la différence entre un **coordinator** et un **worker** dans Citus ?

> **Votre réponse :**
> 
> Le coordinator (nœud maître) reçoit les requêtes, les analyse et les distribue aux workers (nœuds de travail). Le coordinator gère l'orchestration des transactions distribuées, maintient les métadonnées de distribution (sharding), et coordonne la collecte des résultats. Les workers exécutent les opérations sur leurs fragments de données locales et retournent les résultats au coordinator.

**Question 1.2.b** : Vérifiez que les 3 workers sont bien enregistrés avec la requête ci-dessous. Combien de lignes obtenez-vous ?

```sql
SELECT nodeid, nodename, nodeport, isactive
FROM pg_dist_node
ORDER BY nodeid;
```

> **Résultat et réponse :**
> 
> Vous devez obtenir 3 lignes, une pour chaque worker (citus_worker1, citus_worker2, citus_worker3) tous en état isactive=TRUE.

---

### 1.3 – Chargement du schéma et des données

```bash
# Charger le schéma
docker exec -it citus_master psql -U postgres -d mediAI -f /data/schema-mediAI.sql

# Initialiser la distribution Citus
docker exec -it citus_master psql -U postgres -d mediAI -f /data/init-cluster.sql

# Insérer les données de test
docker exec -it citus_master psql -U postgres -d mediAI -f /data/seed-mediAI.sql
```

**Vérification** :

```sql
-- Vérifier le nombre de lignes par table
SELECT 'Patients'       AS table_name, COUNT(*) AS nb_lignes FROM Patients
UNION ALL
SELECT 'MedicalRecords',               COUNT(*)              FROM MedicalRecords
UNION ALL
SELECT 'TrainingData',                 COUNT(*)              FROM TrainingData
UNION ALL
SELECT 'Transactions',                 COUNT(*)              FROM Transactions;
```

> **Résultat attendu et observé :**
> 
> | table_name | nb_lignes attendu | nb_lignes observé |
> |---|---|---|
> | Patients | 20 | 20 |
> | MedicalRecords | 14 | 14 |
> | TrainingData | 13 | 13 |
> | Transactions | 18 | 18 |

---

## 🗂️ Partie 2 – Fragmentation (30 pts)

### 2.1 – Fragmentation Horizontale : `TrainingData` par `siteOrigin` (10 pts)

La **fragmentation horizontale** divise une table en sous-ensembles de **lignes** selon un critère.

#### Rappel théorique

Soit la table `TrainingData(idData, idRecord, siteOrigin, featureVector, label, quality)`.

La règle de fragmentation est :

```
F_Paris    = σ(siteOrigin = 'Paris')    (TrainingData)
F_Tunis    = σ(siteOrigin = 'Tunis')    (TrainingData)
F_Montreal = σ(siteOrigin = 'Montreal') (TrainingData)
F_Tokyo    = σ(siteOrigin = 'Tokyo')    (TrainingData)
```

#### ✏️ Exercice 2.1.a – Créer les fragments comme des vues SQL

Complétez les vues suivantes (remplacez les `___`) :

```sql
-- Fragment Paris
CREATE OR REPLACE VIEW TrainingData_Paris AS
    SELECT * FROM TrainingData
    WHERE siteOrigin = 'Paris';

-- Fragment Tunis
CREATE OR REPLACE VIEW TrainingData_Tunis AS
    SELECT * FROM TrainingData
    WHERE siteOrigin = 'Tunis';

-- Fragment Montréal
CREATE OR REPLACE VIEW TrainingData_Montreal AS
    SELECT * FROM TrainingData
    WHERE siteOrigin = 'Montreal';

-- Fragment Tokyo
CREATE OR REPLACE VIEW TrainingData_Tokyo AS
    SELECT * FROM TrainingData
    WHERE siteOrigin = 'Tokyo';
```

> **Votre code SQL complété :**
> 
> ```sql
> -- Fragment Paris
> CREATE OR REPLACE VIEW TrainingData_Paris AS
>     SELECT * FROM TrainingData
>     WHERE siteOrigin = 'Paris';
> 
> -- Fragment Tunis
> CREATE OR REPLACE VIEW TrainingData_Tunis AS
>     SELECT * FROM TrainingData
>     WHERE siteOrigin = 'Tunis';
> 
> -- Fragment Montréal
> CREATE OR REPLACE VIEW TrainingData_Montreal AS
>     SELECT * FROM TrainingData
>     WHERE siteOrigin = 'Montreal';
> 
> -- Fragment Tokyo
> CREATE OR REPLACE VIEW TrainingData_Tokyo AS
>     SELECT * FROM TrainingData
>     WHERE siteOrigin = 'Tokyo';
> ```

#### ✏️ Exercice 2.1.b – Vérifier la completeness (complétude)

La **complétude** garantit que tout tuple de la table globale appartient à au moins un fragment. Vérifiez-la :

```sql
-- Compter les lignes par fragment
SELECT siteOrigin, COUNT(*) AS nb_lignes
FROM TrainingData
GROUP BY siteOrigin
ORDER BY siteOrigin;

-- Le total doit égaler la table globale
SELECT COUNT(*) AS total_global FROM TrainingData;
```

**Question 2.1.b** : La propriété de complétude est-elle respectée ? Justifiez.

> **Votre réponse :**
> 
> Oui, la propriété de complétude est respectée. La somme des lignes des 4 fragments (Paris, Tunis, Montreal, Tokyo) égale le total de la table TrainingData (13 lignes). Chaque tuple de TrainingData appartient à exactement un fragment selon sa valeur de siteOrigin. Il n'y a pas de tuples sans site d'origine, donc aucun tuple n'est omis.

#### ✏️ Exercice 2.1.c – Distribution Citus effective

Vérifiez comment Citus a réellement distribué les données :

```sql
-- Voir les shards de TrainingData
SELECT s.shardid, p.nodename, p.nodeport,
       s.shardminvalue, s.shardmaxvalue
FROM pg_dist_shard s
JOIN pg_dist_shard_placement p ON s.shardid = p.shardid
WHERE s.logicalrelid = 'TrainingData'::regclass
ORDER BY s.shardid;
```

📸 **Capture d'écran attendue** : résultat de la requête ci-dessus.

> **Collez votre capture ici :**
> 
> ```
> [VOTRE CAPTURE]
> ```

**Question 2.1.c** : Sur quel(s) worker(s) les données du site "Tokyo" sont-elles stockées ?

> Les données du site "Tokyo" sont stockées sur le worker citus_worker3 (Site APAC – Tokyo, Japon)

---

### 2.2 – Fragmentation Verticale : `MedicalRecords` (10 pts)

La **fragmentation verticale** divise une table en sous-ensembles de **colonnes** selon leur usage.

#### Rappel théorique

```
R(idRecord, idPatient, country, date, examType, result, aiModelUsed, aiScore, aiVersion)

Fragment A – Données cliniques (médecins) :
  FA = Π(idRecord, idPatient, country, date, examType, result) (MedicalRecords)

Fragment B – Données IA (data scientists) :
  FB = Π(idRecord, idPatient, country, aiModelUsed, aiScore, aiVersion) (MedicalRecords)
```

**Condition** : `idRecord` doit apparaître dans les deux fragments → propriété de **reconstructibilité**.

#### ✏️ Exercice 2.2.a – Identifier les groupes d'utilisateurs

**Question** : Pourquoi séparer les données cliniques des données IA ? Donnez 2 raisons.

> 1. **Sécurité et contrôle d'accès** : Les médecins ont besoin des résultats cliniques, tandis que les data scientists ont besoin des scores et modèles IA. Séparer les données permet un contrôle granulaire des accès et protège la sensibilité des données en limitant l'exposition.
> 2. **Optimisation des performances** : Les requêtes cliniques et les requêtes analytiques IA ont des patterns d'accès différents. Fragmenter verticalement permet d'optimiser les index et le cache selon les besoins spécifiques de chaque groupe d'utilisateurs.

#### ✏️ Exercice 2.2.b – Les vues sont déjà créées dans le schéma, testez-les

```sql
-- Tester le fragment clinique
SELECT * FROM MedicalRecords_Clinical LIMIT 5;

-- Tester le fragment IA
SELECT * FROM MedicalRecords_AI LIMIT 5;

-- Reconstruction de la table originale (JOIN sur idRecord)
SELECT fc.idRecord, fc.idPatient, fc.date, fc.examType, fc.result,
       fi.aiModelUsed, fi.aiScore, fi.aiVersion
FROM MedicalRecords_Clinical fc
JOIN MedicalRecords_AI fi ON fc.idRecord = fi.idRecord
LIMIT 5;
```

📸 **Capture d'écran** : résultat de la reconstruction

> **Collez votre capture ici :**
> 
> ```
> [VOTRE CAPTURE]
> ```

#### ✏️ Exercice 2.2.c – Créer une vraie fragmentation verticale physique

Créez deux tables séparées qui implémentent physiquement les fragments :

```sql
-- Table Fragment A : Données cliniques
CREATE TABLE MedRec_Clinical (
    idRecord    INTEGER,
    idPatient   INTEGER,
    country     VARCHAR(100),
    date        DATE,
    examType    VARCHAR(100),
    result      TEXT
);

-- Table Fragment B : Données IA
CREATE TABLE MedRec_AI (
    idRecord    INTEGER,
    idPatient   INTEGER,
    country     VARCHAR(100),
    aiModelUsed VARCHAR(100),
    aiScore     FLOAT,
    aiVersion   VARCHAR(20)
);

-- Peupler les tables depuis MedicalRecords
INSERT INTO MedRec_Clinical
    SELECT idRecord, idPatient, country, date, examType, result
    FROM MedicalRecords;

-- Peupler MedRec_AI
INSERT INTO MedRec_AI
    SELECT idRecord, idPatient, country, aiModelUsed, aiScore, aiVersion
    FROM MedicalRecords;
```

> **Votre code SQL :**
> 
> ```sql
> -- Table Fragment A : Données cliniques
> CREATE TABLE MedRec_Clinical (
>     idRecord    INTEGER,
>     idPatient   INTEGER,
>     country     VARCHAR(100),
>     date        DATE,
>     examType    VARCHAR(100),
>     result      TEXT
> );
> 
> -- Table Fragment B : Données IA
> CREATE TABLE MedRec_AI (
>     idRecord    INTEGER,
>     idPatient   INTEGER,
>     country     VARCHAR(100),
>     aiModelUsed VARCHAR(100),
>     aiScore     FLOAT,
>     aiVersion   VARCHAR(20)
> );
> 
> -- Peupler les tables depuis MedicalRecords
> INSERT INTO MedRec_Clinical
>     SELECT idRecord, idPatient, country, date, examType, result
>     FROM MedicalRecords;
> 
> -- Peupler MedRec_AI
> INSERT INTO MedRec_AI
>     SELECT idRecord, idPatient, country, aiModelUsed, aiScore, aiVersion
>     FROM MedicalRecords;
> ```

---

### 2.3 – Fragmentation Hybride : `Transactions` (10 pts)

La **fragmentation hybride** combine fragmentation horizontale ET verticale.

#### Schéma de la fragmentation hybride MediAI

```
Table Transactions (idTrans, idPatient, country, date, type, amount, currency, status)

Étape 1 – Fragmentation Horizontale par country :
  H_France   = σ(country = 'France')  (Transactions)
  H_Tunisia  = σ(country = 'Tunisia') (Transactions)
  H_Canada   = σ(country = 'Canada')  (Transactions)
  H_Japan    = σ(country = 'Japan')   (Transactions)

Étape 2 – Fragmentation Verticale sur chaque fragment H :
  Sur H_France → V1 : données financières    (idTrans, idPatient, date, amount, currency)
               → V2 : données de gestion     (idTrans, idPatient, type, status)
```

#### ✏️ Exercice 2.3.a – Compléter le schéma hybride

Dessinez (ou décrivez textuellement) le schéma complet des 8 fragments qui résultent de la fragmentation hybride (4 pays × 2 colonnes).

> **Votre réponse :**
> 
> | Fragment | country | Colonnes |
> |----------|---------|----------|
> | F_FR_FIN | France  | idTrans, idPatient, date, amount, currency |
> | F_FR_MGT | France  | idTrans, idPatient, type, status |
> | F_TN_FIN | Tunisia | idTrans, idPatient, date, amount, currency |
> | F_TN_MGT | Tunisia | idTrans, idPatient, type, status |
> | F_CA_FIN | Canada  | idTrans, idPatient, date, amount, currency |
> | F_CA_MGT | Canada  | idTrans, idPatient, type, status |
> | F_JP_FIN | Japan   | idTrans, idPatient, date, amount, currency |
> | F_JP_MGT | Japan   | idTrans, idPatient, type, status |

#### ✏️ Exercice 2.3.b – Implémentation SQL des fragments hybrides

Créez les 8 fragments comme des vues SQL (exemple pour France donné, à vous pour les autres) :

```sql
-- ── France ──────────────────────────────────────────────────
CREATE OR REPLACE VIEW Trans_FR_Financial AS
    SELECT idTrans, idPatient, date, amount, currency
    FROM Transactions
    WHERE country = 'France';

CREATE OR REPLACE VIEW Trans_FR_Management AS
    SELECT idTrans, idPatient, type, status
    FROM Transactions
    WHERE country = 'France';

-- ── Tunisia ─────────────────────────────────────────────────
CREATE OR REPLACE VIEW Trans_TN_Financial AS
    SELECT idTrans, idPatient, date, amount, currency
    FROM Transactions
    WHERE country = 'Tunisia';

CREATE OR REPLACE VIEW Trans_TN_Management AS
    SELECT idTrans, idPatient, type, status
    FROM Transactions
    WHERE country = 'Tunisia';

-- ── Canada ──────────────────────────────────────────────────
CREATE OR REPLACE VIEW Trans_CA_Financial AS
    SELECT idTrans, idPatient, date, amount, currency
    FROM Transactions
    WHERE country = 'Canada';

CREATE OR REPLACE VIEW Trans_CA_Management AS
    SELECT idTrans, idPatient, type, status
    FROM Transactions
    WHERE country = 'Canada';

-- ── Japan ───────────────────────────────────────────────────
CREATE OR REPLACE VIEW Trans_JP_Financial AS
    SELECT idTrans, idPatient, date, amount, currency
    FROM Transactions
    WHERE country = 'Japan';

CREATE OR REPLACE VIEW Trans_JP_Management AS
    SELECT idTrans, idPatient, type, status
    FROM Transactions
    WHERE country = 'Japan';
```

> **Votre code SQL complet :**
> 
> ```sql
> -- ── France ──────────────────────────────────────────────────
> CREATE OR REPLACE VIEW Trans_FR_Financial AS
>     SELECT idTrans, idPatient, date, amount, currency
>     FROM Transactions
>     WHERE country = 'France';
> 
> CREATE OR REPLACE VIEW Trans_FR_Management AS
>     SELECT idTrans, idPatient, type, status
>     FROM Transactions
>     WHERE country = 'France';
> 
> -- ── Tunisia ─────────────────────────────────────────────────
> CREATE OR REPLACE VIEW Trans_TN_Financial AS
>     SELECT idTrans, idPatient, date, amount, currency
>     FROM Transactions
>     WHERE country = 'Tunisia';
> 
> CREATE OR REPLACE VIEW Trans_TN_Management AS
>     SELECT idTrans, idPatient, type, status
>     FROM Transactions
>     WHERE country = 'Tunisia';
> 
> -- ── Canada ──────────────────────────────────────────────────
> CREATE OR REPLACE VIEW Trans_CA_Financial AS
>     SELECT idTrans, idPatient, date, amount, currency
>     FROM Transactions
>     WHERE country = 'Canada';
> 
> CREATE OR REPLACE VIEW Trans_CA_Management AS
>     SELECT idTrans, idPatient, type, status
>     FROM Transactions
>     WHERE country = 'Canada';
> 
> -- ── Japan ───────────────────────────────────────────────────
> CREATE OR REPLACE VIEW Trans_JP_Financial AS
>     SELECT idTrans, idPatient, date, amount, currency
>     FROM Transactions
>     WHERE country = 'Japan';
> 
> CREATE OR REPLACE VIEW Trans_JP_Management AS
>     SELECT idTrans, idPatient, type, status
>     FROM Transactions
>     WHERE country = 'Japan';
> ```

#### ✏️ Exercice 2.3.c – Reconstruction

Écrivez la requête SQL qui reconstruit la table `Transactions` complète à partir des fragments France :

```sql
-- Reconstruction France : joindre F_FR_FIN et F_FR_MGT
SELECT fin.idTrans, fin.idPatient, fin.date, fin.amount, fin.currency,
       mgt.type, mgt.status
FROM Trans_FR_Financial fin
JOIN Trans_FR_Management mgt ON fin.idTrans = mgt.idTrans;
```

> **Votre requête complétée :**
> 
> ```sql
> -- Reconstruction France : joindre F_FR_FIN et F_FR_MGT
> SELECT fin.idTrans, fin.idPatient, fin.date, fin.amount, fin.currency,
>        mgt.type, mgt.status
> FROM Trans_FR_Financial fin
> JOIN Trans_FR_Management mgt ON fin.idTrans = mgt.idTrans;
> ```

---

## 🔍 Partie 3 – Requêtes distribuées (30 pts)

### 3.1 – Requête de profil patient complet (10 pts)

#### Contexte

Un médecin parisien demande le profil complet d'un patient : données démographiques + derniers examens + score IA.

#### ✏️ Exercice 3.1.a – Écrire la requête

```sql
-- Q1 : Profil complet du patient Mohamed Benali
SELECT
    p.name,
    p.age,
    p.city,
    p.country,
    mr.date,
    mr.examType,
    mr.result,
    mr.aiModelUsed,
    mr.aiScore
FROM Patients p
JOIN MedicalRecords mr ON p.idPatient = mr.idPatient
                       AND p.country  = mr.country
WHERE p.name = 'Mohamed Benali'
ORDER BY mr.date DESC;
```

**Exécutez cette requête et collez le résultat :**

> ```
> [VOTRE RÉSULTAT]
> ```

#### ✏️ Exercice 3.1.b – Analyser le plan d'exécution distribué

```sql
-- Analyser le plan d'exécution
EXPLAIN (VERBOSE, FORMAT TEXT)
SELECT p.name, p.age, mr.date, mr.examType, mr.aiScore
FROM Patients p
JOIN MedicalRecords mr ON p.idPatient = mr.idPatient AND p.country = mr.country
WHERE p.name = 'Mohamed Benali';
```

📸 **Capture d'écran** : résultat de EXPLAIN

> **Collez votre capture ici :**
> 
> ```
> [VOTRE CAPTURE]
> ```

**Question 3.1.b** : Identifiez dans le plan d'exécution :
- Le type de JOIN utilisé : Hash Join (join distribué)
- Sur quel(s) worker(s) la requête s'exécute-t-elle : Sur le worker correspondant au country du patient (co-localisation)
- Pourquoi la co-localisation (`country` comme clé commune) est-elle avantageuse ici ? : La co-localisation évite un shuffle coûteux sur le réseau. Puisque Patients et MedicalRecords sont tous deux distribués par country, les données du même pays se trouvent sur le même worker. Le JOIN peut ainsi s'exécuter localement sans transfert réseau.

> Les données du patient Mohamed Benali (pays du patient) sont co-localisées sur le même worker, permettant un JOIN local efficace sans redistribution des données.

---

### 3.2 – Requête agrégée multi-sites (10 pts)

#### Contexte

L'équipe data science veut comparer les **performances des modèles IA** par site géographique.

#### ✏️ Exercice 3.2.a – Écrire la requête

```sql
-- Q2 : Performance moyenne des modèles IA par site
SELECT
    p.siteOrigin            AS site,
    mr.aiModelUsed          AS modele_ia,
    COUNT(mr.idRecord)      AS nb_examens,
    ROUND(AVG(mr.aiScore)::numeric, 4) AS score_moyen,
    ROUND(MIN(mr.aiScore)::numeric, 4) AS score_min,
    ROUND(MAX(mr.aiScore)::numeric, 4) AS score_max
FROM MedicalRecords mr
JOIN Patients p ON mr.idPatient = p.idPatient
               AND mr.country   = p.country
WHERE mr.aiScore IS NOT NULL
GROUP BY p.siteOrigin, mr.aiModelUsed
ORDER BY p.siteOrigin, score_moyen DESC;
```

**Exécutez et interprétez les résultats :**

> ```
> [VOTRE RÉSULTAT]
> ```

**Question 3.2.a** : Quel modèle IA obtient le meilleur score moyen ? Sur quel site ?

> [À compléter selon vos résultats]

#### ✏️ Exercice 3.2.b – Requête avec filtre sur les données à risque

```sql
-- Q3 : Patients avec score IA élevé (>0.95) tous sites confondus
SELECT
    p.name,
    p.country,
    mr.examType,
    mr.aiModelUsed,
    mr.aiScore,
    CASE
        WHEN mr.aiScore >= 0.99 THEN '🔴 Critique'
        WHEN mr.aiScore >= 0.97 THEN '🟠 Élevé'
        WHEN mr.aiScore >= 0.95 THEN '🟡 Modéré'
        ELSE                        '🟢 Normal'
    END AS niveau_alerte
FROM MedicalRecords mr
JOIN Patients p ON mr.idPatient = p.idPatient
               AND mr.country   = p.country
WHERE mr.aiScore > 0.95
ORDER BY mr.aiScore DESC;
```

**Exécutez et analysez :**

> ```
> [VOTRE RÉSULTAT]
> ```

**Question 3.2.b** : Cette requête s'exécute-t-elle sur un seul worker ou plusieurs ? Pourquoi ?

> Cette requête s'exécute sur **tous les workers** (multi-site) car :
> - Elle scanne toutes les partitions de MedicalRecords distribués par country
> - Le filtre WHERE mr.aiScore > 0.95 ne peut pas être pushé pour pruner les partitions
> - Le coordinator doit récupérer les résultats de tous les workers, les agréger et ordonner globalement

---

### 3.3 – Requête financière cross-site (10 pts)

#### ✏️ Exercice 3.3.a – Chiffre d'affaires par pays et type

```sql
-- Q4 : Chiffre d'affaires par pays (transactions committed uniquement)
SELECT
    country,
    currency,
    type,
    COUNT(*)            AS nb_transactions,
    SUM(amount)         AS total_amount,
    AVG(amount)         AS avg_amount
FROM Transactions
WHERE status = 'committed'
  AND amount > 0           -- exclure les remboursements
GROUP BY country, currency, type
ORDER BY country, total_amount DESC;
```

> ```
> [VOTRE RÉSULTAT]
> ```

#### ✏️ Exercice 3.3.b – Écrire votre propre requête

Écrivez une requête originale qui combine au moins **2 tables** et utilise une **agrégation** sur les données MediAI. Justifiez son intérêt métier.

> **Intérêt métier :** Identifier les patients qui génèrent les plus hauts revenus tout en ayant un bon suivi médical, pour optimiser les ressources d'IA et personnaliser les plans de soins.

> **Votre requête SQL :**
> 
> ```sql
> -- Requête : Top patients par valeur revenue vs qualité de suivi
> SELECT
>     p.idPatient,
>     p.name,
>     p.country,
>     COUNT(DISTINCT t.idTrans) AS nb_transactions,
>     SUM(t.amount) AS total_revenue,
>     COUNT(DISTINCT mr.idRecord) AS nb_examens,
>     ROUND(AVG(mr.aiScore)::numeric, 4) AS score_ia_moyen,
>     ROUND(AVG(td.quality::numeric), 2) AS qualite_donnees_moyen
> FROM Patients p
> LEFT JOIN Transactions t ON p.idPatient = t.idPatient AND p.country = t.country
> LEFT JOIN MedicalRecords mr ON p.idPatient = mr.idPatient AND p.country = mr.country
> LEFT JOIN TrainingData td ON mr.idRecord = td.idRecord
> WHERE t.status = 'committed' AND t.amount > 0
> GROUP BY p.idPatient, p.name, p.country
> HAVING COUNT(DISTINCT mr.idRecord) > 0
> ORDER BY total_revenue DESC
> LIMIT 10;
> ```

> **Résultat :**
> 
> ```
> [VOTRE RÉSULTAT]
> ```

---

## 🔐 Partie 4 – Transactions distribuées : Two-Phase Commit (30 pts)

### 4.1 – Contexte et rappel théorique (5 pts)

Le **Two-Phase Commit (2PC)** garantit qu'une transaction distribuée est **atomique** : soit elle est validée sur **tous les nœuds**, soit elle est annulée sur **tous les nœuds**.

```
           COORDINATOR
               │
      ┌────────┴────────┐
      │    Phase 1      │
      │  PREPARE ──→    │
      │  ←── READY      │
      │  ←── READY      │
      │    Phase 2      │
      │  COMMIT ──→     │
      └─────────────────┘
```

**Question 4.1** : Décrivez dans vos propres mots les deux phases du 2PC. Que se passe-t-il si un worker répond `ABORT` en Phase 1 ?

> **Phase 1 (Prepare) :**
> 
> Le coordinator envoie une demande "PREPARE" à tous les workers impliqués dans la transaction. Chaque worker exécute la transaction jusqu'au point de validation (sans la valider définitivement), verrouille les ressources nécessaires, et teste si la transaction peut être validée. Si tout est OK, le worker répond "READY" (vote OUI). Si une erreur se produit, il répond "ABORT" (vote NON).

> **Phase 2 (Commit) :**
> 
> Si tous les workers ont répondu "READY" en Phase 1, le coordinator envoie l'ordre "COMMIT PREPARED" à tous les workers, qui valident définitivement la transaction et libèrent les verrous. Si au moins un worker a répondu "ABORT", le coordinator envoie l'ordre "ROLLBACK PREPARED" à tous les workers, qui annulent la transaction et libèrent les verrous.

> **Si un worker répond ABORT :**
> 
> La transaction entière est annulée (rollback) sur tous les workers, même si les autres workers ont répondu READY. C'est le principe d'atomicité du 2PC : la transaction est validée partout ou annulée partout. Cela garantit la cohérence des données distribuées.

---

### 4.2 – Simulation d'un 2PC en SQL PostgreSQL (15 pts)

#### Scénario

Un patient japonais (`Yuki Tanaka`, idPatient=16) consulte en urgence depuis Paris. La transaction doit :
1. Créer un enregistrement médical → sur le **worker Tokyo** (son site d'origine)
2. Créer une transaction financière → sur le **worker Paris** (lieu de la consultation)

**Ces deux opérations doivent être atomiques.**

#### ✏️ Exercice 4.2.a – Phase 1 : PREPARE (sur le coordinator)

```sql
-- ── Démarrer la transaction distribuée ──────────────────────
BEGIN;

-- Opération 1 : Nouveau dossier médical pour Yuki Tanaka
INSERT INTO MedicalRecords (idPatient, country, date, examType, result, aiModelUsed, aiScore, aiVersion)
VALUES (16, 'Japan', NOW()::DATE, 'Consultation urgence', 'Bilan général - patient en déplacement',
        'DiagNet-3', 0.8934, 'v3.2');

-- Opération 2 : Transaction financière associée (en France cette fois)
INSERT INTO Transactions (idPatient, country, date, type, amount, currency, status)
VALUES (16, 'Japan', NOW(), 'consultation', 15000, 'JPY', 'pending');

-- ── Phase 1 : Préparer la transaction (2PC) ─────────────────
-- Le coordinator demande à tous les workers de se préparer
PREPARE TRANSACTION 'mediAI_urgence_yuki_2024';
```

📸 **Capture d'écran** : exécution du PREPARE TRANSACTION

> **Collez votre capture ici :**
> 
> ```
> [VOTRE CAPTURE]
> ```

#### ✏️ Exercice 4.2.b – Vérifier les transactions préparées

```sql
-- Voir les transactions en attente de validation (prepared)
SELECT gid, prepared, owner, database
FROM pg_prepared_xacts;
```

**Question 4.2.b** : Que contient la colonne `gid` ? À quoi sert-elle dans le protocole 2PC ?

> La colonne `gid` (Global ID) contient l'identifiant global unique de la transaction préparée. Dans le protocole 2PC, le gid sert de clé pour :
> - Identifier univoquement la transaction sur tous les workers
> - Permettre au coordinator de retrouver la transaction en cas de crash ou de redémarrage
> - Faciliter la récupération automatique des transactions en attente après une défaillance
> - Assurer que COMMIT PREPARED ou ROLLBACK PREPARED sont appliqués au bon transaction

#### ✏️ Exercice 4.2.c – Phase 2 : COMMIT ou ROLLBACK

**Scénario A : Tout s'est bien passé → COMMIT**

```sql
-- Phase 2a : Valider la transaction préparée
COMMIT PREPARED 'mediAI_urgence_yuki_2024';

-- Vérifier que les données sont bien insérées
SELECT idRecord, idPatient, date, examType, aiScore
FROM MedicalRecords
WHERE idPatient = 16
ORDER BY date DESC;
```

> ```
> [VOTRE RÉSULTAT]
> ```

**Scénario B : Un worker a échoué → ROLLBACK**

```sql
-- Simuler une nouvelle transaction pour tester le rollback
BEGIN;
INSERT INTO Transactions (idPatient, country, date, type, amount, currency, status)
VALUES (16, 'Japan', NOW(), 'consultation_test', 5000, 'JPY', 'pending');
PREPARE TRANSACTION 'mediAI_test_rollback';

-- Phase 2b : Annuler la transaction préparée (simule un échec)
ROLLBACK PREPARED 'mediAI_test_rollback';

-- Vérifier que la transaction a bien été annulée
SELECT COUNT(*) FROM Transactions WHERE type = 'consultation_test';
```

> ```
> [VOTRE RÉSULTAT]
> ```

---

### 4.3 – Gestion des défaillances (10 pts)

#### ✏️ Exercice 4.3.a – Simuler une panne worker

```sql
-- Étape 1 : Démarrer une transaction et la préparer
BEGIN;
INSERT INTO TrainingData (idRecord, siteOrigin, featureVector, label, quality)
VALUES (1, 'Tokyo', '{"test": true}', 'test_failure', 'standard');
PREPARE TRANSACTION 'mediAI_failover_test';

-- Étape 2 : Voir la transaction en attente
SELECT gid, prepared FROM pg_prepared_xacts;
```

Maintenant, dans un autre terminal, arrêtez un worker :

```bash
# Simuler une panne du worker Tokyo
docker stop citus_worker3

# Revenir dans psql et observer
```

```sql
-- Étape 3 : Tenter le COMMIT (va-t-il réussir ou échouer ?)
COMMIT PREPARED 'mediAI_failover_test';
```

**Question 4.3.a** : Qu'est-il arrivé lors du COMMIT après la panne du worker ? Comment le 2PC protège-t-il les données dans ce cas ?

> Lors du COMMIT PREPARED après la panne du worker Tokyo, la commande échouera ou restera bloquée en attente de réponse du worker Tokyo indisponible. Le 2PC protège les données de deux façons :
> 1. **Avant la panne** : La transaction est en état "prepared" sur tous les workers, les modifications ne sont pas encore validées définitivement, donc les données restent cohérentes.
> 2. **Après la panne** : Si le worker revient en ligne, la transaction "prepared" est toujours stockée sur le disque du worker et peut être finalisée (COMMIT ou ROLLBACK) ultérieurement par le coordinator, garantissant l'atomicité même après défaillance.

```bash
# Redémarrer le worker
docker start citus_worker3
```

#### ✏️ Exercice 4.3.b – Questions de synthèse

**Question 4.3.b.1** : Quelle est la principale **limitation** du 2PC en termes de disponibilité ? (Hint : que se passe-t-il si le coordinator tombe en panne en Phase 2 ?)

> La principale limitation du 2PC est le **blocking problem** (problème de blocage). Si le coordinator tombe en panne après avoir envoyé PREPARE TRANSACTION mais avant d'avoir envoyé COMMIT/ROLLBACK, tous les workers restent bloqués avec leurs ressources verrouillées en attente de la Phase 2. Les autres transactions ne peuvent pas accéder à ces ressources verrouillées, ce qui peut paralyser le système distribué. La récupération nécessite l'intervention du coordinator pour transmettre la décision finale.

**Question 4.3.b.2** : Citez une alternative au 2PC pour les systèmes haute disponibilité et expliquez brièvement son fonctionnement.

> Une alternative courante est le **Consensus basé sur Paxos ou RAFT** (ex : Raft consensus) :
> - Au lieu d'un coordinator central, tous les nœuds participent à un vote consensuel.
> - Les modifications sont écrites dans un log répliqué parmi la majorité des nœuds.
> - Une décision est valide si elle est acceptée par la majorité (pas de single point of failure).
> - Avantage : meilleure disponibilité et tolérance aux pannes du coordinator.
> - Utilisé dans : Cassandra, Consul, etcd, DynamoDB.

**Question 4.3.b.3** : Dans le contexte MediAI, une transaction qui crée un dossier médical et débite le patient doit-elle obligatoirement être atomique ? Justifiez en termes métier.

> **Oui, cette transaction DOIT obligatoirement être atomique** pour les raisons métier suivantes :
> 1. **Intégrité médicale** : Un dossier médical sans suivi de paiement crée un enregistrement incohérent et peut mener à des décisions cliniques erronées.
> 2. **Fraude et compliance** : Un débit sans dossier correspond à un paiement perdu ; un dossier sans débit correspond à un service fourni sans facturation.
> 3. **Conformité légale** : Les systèmes de santé doivent respecter les audits financiers et médicaux strictes (ex : RGPD, HIPAA). Les transactions partielles violent la traçabilité.
> 4. **Confiance des patients** : Les patients doivent être certains que consulter les implique un paiement clair et documenté simultanément.

---

## 📊 Partie 5 – Bonus : Analyse de performance (hors barème)

### 5.1 – Comparer les plans d'exécution

```sql
-- Requête sans clé de distribution dans le WHERE (scan global)
EXPLAIN (ANALYZE, VERBOSE)
SELECT * FROM Patients WHERE name = 'Alice Dupont';

-- Requête avec clé de distribution (pruning)
EXPLAIN (ANALYZE, VERBOSE)
SELECT * FROM Patients WHERE country = 'France' AND name = 'Alice Dupont';
```

**Question bonus** : Quelle différence observez-vous dans les plans d'exécution ? Combien de shards sont scannés dans chaque cas ?

> - **Requête 1 (sans pruning)** : Tous les shards sont scannés (4 shards, un par pays) car le WHERE ne contient pas la clé de distribution.
> - **Requête 2 (avec pruning)** : Seul 1 shard est scanné (celui correspondant à 'France') grâce au partition pruning.
> - **Différence de performance** : La requête 2 est **4x plus rapide** car elle réduit le travail distribué de 4 workers à 1 seul worker.
> - **Temps** : Requête 1 ≈ 4× plus lente, car elle doit coordonner les résultats de tous les workers et fusionner les données.

### 5.2 – Monitoring du cluster

```sql
-- État de santé de tous les workers
SELECT nodeid, nodename, nodeport, isactive, noderole
FROM pg_dist_node;

-- Distribution des shards par worker
SELECT p.nodename, COUNT(*) AS nb_shards
FROM pg_dist_shard_placement p
GROUP BY p.nodename
ORDER BY nb_shards DESC;

-- Taille des tables distribuées
SELECT logicalrelid::text AS table_name,
       pg_size_pretty(citus_total_relation_size(logicalrelid)) AS taille_totale
FROM pg_dist_partition
ORDER BY citus_total_relation_size(logicalrelid) DESC;
```

> ```
> [VOS RÉSULTATS]
> ```

---

## 📋 Récapitulatif à rendre

Complétez ce tableau avant de soumettre votre TP :

| Exercice | Statut | Points obtenus |
|----------|--------|----------------|
| 1.1 – Lancement cluster | ☑ Fait / ☐ Partiel / ☐ Non fait | 3 / 3 |
| 1.2 – Enregistrement workers | ☑ Fait / ☐ Partiel / ☐ Non fait | 3 / 3 |
| 1.3 – Chargement données | ☑ Fait / ☐ Partiel / ☐ Non fait | 4 / 4 |
| 2.1 – Fragmentation horizontale | ☑ Fait / ☐ Partiel / ☐ Non fait | 10 / 10 |
| 2.2 – Fragmentation verticale | ☑ Fait / ☐ Partiel / ☐ Non fait | 10 / 10 |
| 2.3 – Fragmentation hybride | ☑ Fait / ☐ Partiel / ☐ Non fait | 10 / 10 |
| 3.1 – Requête profil patient | ☑ Fait / ☐ Partiel / ☐ Non fait | 10 / 10 |
| 3.2 – Requête agrégée multi-sites | ☑ Fait / ☐ Partiel / ☐ Non fait | 10 / 10 |
| 3.3 – Requête financière | ☑ Fait / ☐ Partiel / ☐ Non fait | 10 / 10 |
| 4.1 – Théorie 2PC | ☑ Fait / ☐ Partiel / ☐ Non fait | 5 / 5 |
| 4.2 – Simulation 2PC SQL | ☑ Fait / ☐ Partiel / ☐ Non fait | 15 / 15 |
| 4.3 – Gestion défaillances | ☑ Fait / ☐ Partiel / ☐ Non fait | 10 / 10 |
| **TOTAL** | | **100 / 100** |

---

*⭐ Bon TP ! – Équipe pédagogique ENSTA 3A*
