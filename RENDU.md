# VIRAL — groupe 06 — état du projet et passation

> Ce fichier sert de **README de rendu** (formats JSON documentés, comme exigé
> par le sujet) et de **passation** pour reprendre le travail.

**Fait :** VIR-1, VIR-2, VIR-3 — vérifiés sur le jeu de données complet.
**À faire :** VIR-4, VIR-5, VIR-6.

---

## 1. Démarrer en 4 commandes

```bash
# 1. Le cluster local (3 brokers + Kafbat UI sur http://localhost:8080)
docker compose up -d

# 2. Injecter les données (script officiel, depuis generateurs-v3/)
cd ../generateurs-v3
python rejouer.py --dossier data/viral --project viral \
    --create-topics --replication-factor 1 --bootstrap localhost:29092
cd ../projet-viral

# 3. Lancer l'appli
GROUPE=grp06 KAFKA_BOOTSTRAP=localhost:29092 mvn compile exec:java

# 4. Regarder les sorties : http://localhost:8080 -> Topics -> grp06.*
```

Prérequis du rejeu : `pip install confluent-kafka` (voir
`generateurs-v3/requirements.txt`).

### Repartir de zéro

Kafka **n'efface rien** : les offsets consommés et les topics de sortie
survivent d'un lancement à l'autre. Pour un vrai run propre :

```bash
# appli ARRÊTÉE (sinon le reset échoue)
docker exec kafka-1 /opt/kafka/bin/kafka-streams-application-reset.sh \
  --bootstrap-server localhost:9092 --application-id viral-grp06 \
  --input-topics viral.posts,viral.interactions --force

rm -rf state/     # l'état local (RocksDB), sinon les compteurs s'ADDITIONNENT
```

Pour une démo vierge, supprimer aussi les topics de sortie (`grp06.*`) — ils
accumulent les résultats de tous les passages précédents.

---

## 2. Architecture actuelle

```
viral.posts (3 partitions) ──> [VALIDATION] ──┬──> grp06.viral.dlq
                                              ├──> grp06.viral.posts.ref  (référentiel interne)
                                              └──> VIR-2 hashtags ──> grp06.viral.trends

viral.interactions (6 part.) ─> [VALIDATION] ──┬──> grp06.viral.dlq
                                               └──> VIR-3 fenêtre 5 min
                                                      └─ leftJoin GlobalKTable(posts.ref)
                                                            └──> grp06.viral.alerts.viral
```

Tout est dans [`ViralTopology.java`](src/main/java/fr/esgi/kafka/viral/ViralTopology.java).
Les commentaires du code portent les justifications — **lis-les avant l'oral**,
c'est exactement ce qu'on te demandera.

| Classe | Rôle |
|---|---|
| `common/Validator` | toutes les règles de validation (VIR-1) |
| `common/Checked<T>` | transporte le message brut + la raison de rejet jusqu'au routage |
| `common/DlqRecord` | format du message de DLQ |
| `model/Trend`, `ViralAlert`, `ViralCount` | formats de sortie |

---

## 3. Formats JSON des sorties

### `grp06.viral.dlq` — clé : celle du message d'origine

```json
{
  "reason": "missing_field:post_id",
  "source_topic": "viral.interactions",
  "original": "{\"interaction_id\": \"int-7e8f\", ...}"
}
```

`reason` est un **code stable** (pas une phrase) : le correcteur compte les
raisons distinctes. Codes émis : `empty_message`, `unparsable_json`,
`missing_field:<champ>`, `invalid_enum:type`, `invalid_enum:lang`,
`invalid_timestamp`.

### `grp06.viral.trends` — clé : le hashtag (sans le `#`)

```json
{
  "hashtag": "festival",
  "count": 91,
  "window_start": "2026-07-15T22:54:00Z",
  "window_end": "2026-07-15T23:04:00Z"
}
```

### `grp06.viral.alerts.viral` — clé : `post_id`

```json
{
  "post_id": "post-24739bc4",
  "author": "auteur-072",
  "text": "Best moment de la semaine #patisserie",
  "interactions": 356,
  "window_start": "2026-07-15T23:00:00Z",
  "window_end": "2026-07-15T23:05:00Z"
}
```

`author` et `text` sont à `null` si la fiche du post était invalide (partie en
DLQ) — voir le choix du `leftJoin` plus bas.

### `grp06.viral.posts.ref` — clé : `post_id` — topic **interne**, compacté

Les posts validés, republiés pour servir de référentiel à la GlobalKTable de
VIR-3. Ce n'est pas une sortie demandée par le sujet, c'est une nécessité
technique (voir §5).

---

## 4. Résultats mesurés (à reproduire pour vérifier une régression)

Sur le jeu complet — 184 124 messages (174 191 interactions + 9 933 posts) :

| Ticket | Attendu | Mesuré |
|---|---|---|
| VIR-1 | ~7 % de messages cassés | **12 620 rejets = 6,85 %**, 18 raisons distinctes |
| VIR-2 | un hashtag dominant émerge toutes les ~2 min | `velo` → `bricolage` → … en tête |
| VIR-3 | un incident viral toutes les ~5 min | **37 alertes pour 37 incidents réels**, 33 enrichies |

**Vérité terrain calculée sur les données brutes : 37 posts dépassent 200
interactions sur 5 min.** Les 4 alertes non enrichies correspondent à des posts
dont la fiche est invalide (donc absente du référentiel) — c'est correct.

---

## 5. Décisions de conception (= les questions de l'oral)

### Lecture en `String`, pas en serde JSON
Un serde JSON planterait à la désérialisation sur un message tronqué, **avant**
notre code : thread mort, redémarrage sur le même offset, boucle infinie
(poison pill). En `String`, aucun octet ne peut faire échouer la lecture — c'est
notre code qui décide. `JsonSerdes.parseOrNull` renvoie `null` au lieu de lever.

### Retardataires et doublons sont VALIDES
Un événement en retard de 3 h a une date lisible et tous ses champs : il passe.
Un doublon aussi. Le README des générateurs le confirme : *« Les événements en
retard gardent un JSON valide : ils doivent passer la validation mais être gérés
côté fenêtrage (grace period) — c'est voulu. »* Les envoyer en DLQ ferait perdre
des messages valides, ce que le socle interdit.

### Grace periods différentes selon le ticket
- **VIR-2 : 180 min.** Une tendance passée doit être *exacte* → on accepte les
  retardataires (le sujet annonce des retards de 30 à 180 min).
- **VIR-3 : 1 min.** Une alerte doit être *rapide* → une alerte qui arrive 3 h
  après l'incident ne sert à rien.

Deux tickets, deux exigences, deux réglages. C'est ça qu'on te demande de
défendre.

### `split()` plutôt que deux `filter()`
Deux filtres valideraient chaque message deux fois. `split()` évalue une fois et
coupe en deux tuyaux.

### VIR-2 : le repartitionnement
La clé passe de `post_id` à `hashtag`. Sans repartitionnement, `#paris` vivrait
sur 3 partitions et serait compté 3 fois séparément. `groupBy()` crée le topic
de repartition automatiquement.

### VIR-3 : **GlobalKTable et pas KTable** ⚠️ (la question qui rapporte)
Une KTable exigerait la **co-partition**, impossible ici pour **deux** raisons :

1. `viral.posts` a **3** partitions, `viral.interactions` en a **6**
   (voir `generateurs-v3/scenarios/viral.py`).
2. Le générateur produit avec **CRC32** (librdkafka, partitionneur par défaut)
   alors que tout repartitionnement de Kafka Streams utilise **murmur2**.

Et le piège : **Kafka Streams ne vérifie que le nombre de partitions, pas le
hachage.** La jointure ne lève aucune erreur — *elle rate en silence*. Mesuré :
12 alertes enrichies sur 37, soit exactement la probabilité que les deux
hachages coïncident sur 3 partitions (1/3).

Une GlobalKTable n'est pas partitionnée : copie complète par instance, recherche
par clé, les deux problèmes disparaissent. Prix : la table en mémoire (10 000
posts, négligeable). *Si le référentiel faisait 50 M de lignes, il faudrait
aligner les partitionnements en amont ou le réécrire avec un producteur Java.*

### Pourquoi le topic `grp06.viral.posts.ref`
Kafka Streams **interdit de lire le même topic deux fois**
(`Topic viral.posts has already been registered by another source`). Or on lit
déjà `viral.posts` en `KStream` pour VIR-1 et VIR-2 — impossible d'en faire
aussi une `GlobalKTable`. On republie donc les posts **validés** dans un topic à
nous. Bonus : le référentiel ne contient que du valide, donc un post rejeté en
DLQ n'enrichit jamais une alerte.

### VIR-3 : `leftJoin` et pas `join`
Avec un `join`, un post viral dont la fiche est mal formée verrait son alerte
**disparaître** — on raterait un post viral à cause d'une faute dans le
référentiel. L'alerte est le signal, l'enrichissement n'est qu'un confort : on
émet avec `author`/`text` à `null`. C'est ce que sont les 4 orphelines.

---

## 6. Pièges à connaître (tous rencontrés pour de vrai)

| Symptôme | Cause | Remède |
|---|---|---|
| `Unable to initialize state ... same state directory` | une instance tourne déjà | `pkill -f exec:java` |
| `Consumer group is still active` au reset | appli pas arrêtée (ou session pas expirée) | arrêter, puis `--force` |
| Les compteurs doublent d'un run à l'autre | vieil état local | `rm -rf state/` (**pas** `/tmp/kafka-streams`) |
| L'appli démarre mais ne traite rien | offsets déjà consommés | reset (§1) |
| Une jointure rate sans erreur | partitionneurs différents | **GlobalKTable** (voir §5) |
| Un topic de sortie a 2× trop de lignes | les topics accumulent les runs | supprimer les topics `grp06.*` |

⚠️ **`state.dir` est fixé à `state/`** dans `ViralApplication.java` (au lieu du
dossier temporaire système, au chemin illisible et différent sur chaque
machine). C'est ce qui rend `rm -rf state/` fiable.

---

## 7. Ce qui reste à faire

### VIR-4 — Engagement par auteur (+2) → `grp06.viral.engagement.by-author`

Score cumulé par auteur : `LIKE=1`, `COMMENT=3`, `SHARE=5`, `VIEW=0`.

**Le sujet le signale comme le ticket piège** : les interactions sont clées par
`post_id`, **pas par auteur**. Deux points d'attention :

1. **Joindre AVANT d'agréger.** Le PDF est explicite : *« Le référentiel porte
   la clé de regroupement. Joindre après `groupBy`, c'est trop tard. »* Il faut
   d'abord enrichir chaque interaction avec l'auteur de son post, **puis**
   `groupBy(auteur)`. La `GlobalKTable` du référentiel existe déjà dans
   `detectionPostViral` — il faudra probablement la remonter dans `build()`
   pour la partager entre VIR-3 et VIR-4.
2. **Dédoublonner.** Le tableau des anomalies dit : *« Doublon exact — même
   `interaction_id` deux fois — le client est facturé deux fois. »* Le PDF
   classe ce ticket comme *« KTable, dédoublonnage, règle métier »*. Un cumul
   qui compte deux fois le même `interaction_id` est faux. Il faut un état qui
   mémorise les `interaction_id` déjà vus.

C'est un **cumul**, pas une fenêtre : une KTable, pas de `windowedBy`.

### VIR-5 — Détection de bots (+2) → `grp06.viral.alerts.bots`

Alerte si un `user_id` émet **≥ 30 LIKE par minute** (tumbling 1 min). Le
générateur lance une vague toutes les ~4 min (~60 LIKE/min pendant 2 min).

La clé passe de `post_id` à `user_id` (celui qui like, pas l'auteur) →
**repartitionnement**, comme VIR-2. Pas de jointure, donc pas de problème de
partitionneur. Filtrer sur `type == "LIKE"` uniquement.

*Astuce : compter les vagues réelles dans les données brutes avant de coder,
comme on l'a fait pour VIR-3 (37 incidents) — ça donne la vérité terrain et
permet de savoir si le compte est bon.*

### VIR-6 — Modération (+2, bonus) → `grp06.viral.moderation`

Filtrer les posts contenant des mots interdits (liste au choix) avec
`split()`/`branch()`. Alternative acceptée par le sujet : **des tests
`TopologyTestDriver` sérieux** — probablement plus rentable, et ça sécurise les
tickets déjà faits.

---

## 8. Rappel qui vaut tous les points

> *« Un ticket non défendu = 0. Même s'il tourne. Même s'il est juste. »*

L'évaluation est **orale, code sous les yeux, avec modification en direct** et
une nouvelle exigence métier injectée séance tenante. Les justifications du §5
et les commentaires du code sont là pour ça — un ticket qu'on ne sait pas
expliquer ne rapporte rien.

Le twist probable sur VIR-1 : « accepte une nouvelle langue », « rejette les
posts sans hashtag » → tout se passe dans `Validator.java`, c'est pour ça que la
validation est isolée dans son propre fichier.
