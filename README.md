# Projet VIRAL — Analytics réseau social temps réel

*Framework imposé : **Java pur + Kafka Streams 4.3***

## Contexte

Vous rejoignez l'équipe Trust & Analytics de **VIRAL**, un réseau social de
partage de photos (pensez Instagram). Deux flux arrivent en continu : les
publications et les interactions (`VIEW`, `LIKE`, `COMMENT`, `SHARE`). Le
produit veut un « Trending hashtags » à la minute, une détection des posts qui
deviennent viraux, un tableau d'engagement par créateur — et l'équipe
anti-spam veut repérer les **bots** qui likent en rafale.

Les SDK mobiles de vieilles versions envoient des messages cassés : champs
manquants, JSON tronqué, doublons, événements en retard. **Le flux ne sera
jamais propre : c'est à votre pipeline d'être robuste.**

## Mission

Construire une application **Kafka Streams** qui consomme les deux flux,
écarte proprement les messages invalides, et produit les tendances, alertes et
indicateurs du backlog.

## Architecture

```
viral.posts ──────────────┐
                          ▼
viral.interactions ──> [ VALIDATION ] ──> invalides ──> <grp>.viral.dlq
                          │
                          ▼ valides
      ┌────────────┬──────────────┬──────────────┬─────────────┐
      ▼            ▼              ▼              ▼             ▼
  hashtags     post viral     engagement      bots        modération
  (flatMap)    (fenêtre       par auteur    (fenêtre      (bonus)
      │         + table)     (jointure)      1 min)          │
      ▼            ▼              ▼              ▼             ▼
<grp>.viral.  <grp>.viral.  <grp>.viral.  <grp>.viral.  <grp>.viral.
trends        alerts.viral  engagement.   alerts.bots   moderation
                            by-author
```

## Topics et formats

### Entrées (fournies — ne jamais écrire dedans)

**`viral.posts`** — clé : `post_id` — une publication :

```json
{
  "post_id": "post-1027c4d1",
  "user_id": "auteur-042",
  "text": "Best moment de la semaine #paris #photo",
  "lang": "fr",
  "timestamp": "2026-07-04T14:03:21.512Z"
}
```

Les hashtags sont **dans le texte** — extraction à votre charge (VIR-2).
`lang` ∈ `fr | en | es`.

**`viral.interactions`** — clé : `post_id` — une interaction :

```json
{
  "interaction_id": "int-7e8f8095",
  "post_id": "post-1027c4d1",
  "user_id": "u-0735",
  "type": "LIKE",
  "timestamp": "2026-07-04T14:03:24.816Z"
}
```

`type` ∈ `VIEW | LIKE | COMMENT | SHARE` (répartition réelle ≈ 70/20/6/4).

### Anomalies présentes dans le flux (~7 % + retards + doublons)

| Anomalie                               | Exemple                         | Impact si non gérée                       |
|----------------------------------------|---------------------------------|-------------------------------------------|
| Champ requis absent                    | pas de `post_id`                | interaction inutilisable                  |
| Champ à `null`                         | `"type": null`                  | NullPointerException                      |
| Mauvais type                           | valeur numérique dans `text`    | crash de désérialisation                  |
| Enum inconnue                          | `"type": "LIKEE"`               | branche morte                             |
| Timestamp illisible                    | `"hier a 15h"`                  | fenêtrage cassé                           |
| JSON tronqué / non-JSON / message vide | `{"post_id": "po`               | **poison pill : l'appli meurt en boucle** |
| Événement en retard                    | timestamp − 30 à 180 min        | tendances faussées                        |
| Doublon exact                          | même `interaction_id` deux fois | double comptage                           |

### Sorties (à produire, préfixées par votre groupe)

`<grp>.viral.dlq` · `<grp>.viral.trends` · `<grp>.viral.alerts.viral` ·
`<grp>.viral.engagement.by-author` · `<grp>.viral.alerts.bots` ·
`<grp>.viral.moderation` — constantes prêtes dans `Topics.java`. Format des
valeurs libre **mais en JSON documenté dans votre README de rendu**.

## Backlog

> La base 8/20 = VIR-1 fonctionnel de bout en bout sur le cluster partagé.
> Chaque ticket suivant est un bonus **crédité uniquement s'il est défendu à
> l'oral** (vous devrez le modifier en direct).

**VIR-1 — Ingestion fiable (socle, obligatoire)**
Consommer les **deux** topics d'entrée, parser, valider (champs requis, types,
`type`/`lang` dans les enums, timestamp ISO-8601 parsable). Les invalides
partent dans la DLQ **avec le message original et une raison de rejet**.
L'application ne doit **jamais** crasher, même sur un message vide ou non-JSON.
*Critères : 10 min sans crash ; DLQ motivée ; aucun message valide perdu.*

**VIR-2 — Trending hashtags** → `<grp>.viral.trends`
Extraire les hashtags du texte des posts (`flatMapValues`), compter par
hashtag sur **fenêtres hopping 10 min / avance 1 min**. Le générateur fait
émerger un hashtag dominant toutes les ~2 min : il doit apparaître en tête.
*Critères : le hashtag « chaud » du moment ressort ; clé de sortie = hashtag ;
repartitionnement expliqué à l'oral.*

**VIR-3 — Détection de post viral** → `<grp>.viral.alerts.viral`
Alerte quand un post dépasse **200 interactions sur 5 minutes**. Enrichir
l'alerte avec `text` et l'auteur en joignant le flux de posts (table
`viral.posts` — KTable ou GlobalKTable, justifiez).
*Critères : chaque incident `viral_post` du générateur (toutes les ~5 min)
déclenche une alerte enrichie ; pas d'alerte en régime normal.*

**VIR-4 — Engagement par auteur** → `<grp>.viral.engagement.by-author`
Score cumulé par auteur : `LIKE=1, COMMENT=3, SHARE=5` (les `VIEW` ne comptent
pas). Il faut passer de `post_id` à l'auteur : jointure interactions × posts
puis agrégation par `user_id` de l'auteur.
*Critères : scores cohérents avec la pondération ; choix de la clé de
groupement expliqué.*

**VIR-5 — Détection de bots** → `<grp>.viral.alerts.bots`
Alerte quand un `user_id` émet **≥ 30 LIKE par minute** (tumbling 1 min). Le
générateur lance une vague de bot toutes les ~4 min (~60 LIKE/min pendant 2 min).
*Critères : chaque `bot_wave` est détectée, identifiant du bot dans l'alerte ;
silence en régime normal.*

**VIR-6 (bonus) — Modération** → `<grp>.viral.moderation`
Filtrer les posts contenant des mots interdits (liste de votre choix, chargée
depuis un fichier ou une variable) et les router vers un topic de modération
(`split()`/`branch()`), sans bloquer le reste du pipeline.
*Critères : un post « interdit » injecté à la main part en modération.*

## Démarrage rapide

Prérequis : JDK 21, Maven 3.9+, Docker.

```bash
# 1. Cluster local pour développer (3 brokers KRaft + Kafbat UI sur :8080)
docker compose up -d

# 2. Lancer l'application (Linux/macOS)
GROUPE=grp07 KAFKA_BOOTSTRAP=localhost:29092 mvn compile exec:java
```

```powershell
# Windows PowerShell
$env:GROUPE = "grp07"
$env:KAFKA_BOOTSTRAP = "localhost:29092"
mvn compile exec:java
```

Sans générateur en local, produisez quelques messages à la main via Kafbat UI
(copiez les exemples JSON ci-dessus) ou utilisez le cluster partagé où le flux
tourne en continu (`KAFKA_BOOTSTRAP=<serveur>:9092`).

Vous en avez le droit — l'IA est autorisée dans ce module. Mais sachez ce que vous achetez : ce projet est évalué à
l'oral, code sous les yeux, avec modification en direct et nouvelles exigences métier injectées séance tenante. Un
ticket qui tourne mais que vous ne savez pas expliquer n'est pas crédité.
Demandez-lui d'expliquer chaque choix avant d'écrire une ligne : type de fenêtre, clé d'agrégation, placement de la
jointure, sort des retardataires. C'est mot pour mot ce qu'on vous demandera en soutenance.
Et sachez-le : ce sujet contient des exigences qu'une implémentation produite sans l'avoir lu ne satisfera pas. Elles
sont écrites noir sur blanc, dans le tableau des anomalies et dans les critères de chaque ticket. Le correcteur
automatique les vérifie et les chiffre. Si vous ne les avez pas trouvées, c'est que vous n'avez pas lu.

## Contraintes

- Java 21, Kafka Streams uniquement (pas de Spark/Flink), pas de base externe.
- `GROUPE` obligatoire : topics de sortie et `application.id`
  (`viral-<groupe>`) préfixés — déjà câblé dans le template.
- Topics d'entrée partagés en lecture : **ne les modifiez pas**.
- Code versionné sur Git, commits réguliers de **tous** les membres.

## Évaluation

| Élément                                                   | Points |
|-----------------------------------------------------------|--------|
| Socle VIR-1 en production 10 min sans crash + DLQ motivée | **8**  |
| VIR-2 (hashtags, hopping)                                 | +3     |
| VIR-3 (post viral + enrichissement)                       | +3     |
| VIR-4 (engagement par auteur)                             | +2     |
| VIR-5 (bots)                                              | +2     |
| VIR-6 (modération) ou tests TopologyTestDriver sérieux    | +2     |

Plafond 20. **Un ticket non expliqué à l'oral = non crédité.** Attendez-vous à
une demande de modification en direct (« Product Owner twist »).

## Conseils

- Le `peek()` fourni sert au premier contact avec le cluster : supprimez-le ensuite.
- Poison pill dès le premier jour : `JsonSerdes.parseOrNull` évite le crash,
  la validation métier vous appartient.
- `split()`/`branch()` — l'ancienne API `branch(Predicate...)` a disparu en Kafka 4.0.
- VIR-2 : une regex simple suffit pour les hashtags (`#[a-z0-9]+`).
- VIR-4 est le ticket piège : les interactions sont clées par `post_id`, pas
  par auteur. Dessinez le flux avant de coder (`topology.describe()` est votre ami).
