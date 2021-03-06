# Projet de fin de cours - Sujet n°

Nous allons faire un éternel nouvel essai sur la détection de sentiments sur les tweets.

# Éléments

Source de données

Un topic nommé tweets est disponible sur le broker Kafka du cours, contenant des messages ayant cette
structure:

## {

```
"id": "35c5b656-9c28-48f0-96c3-55db554eb1d1",
"timestamp":"2019-03-02T19:15:49.375558Z",
"nick": "IrisJumbe",
"body": "Oh man...was ironing @jeancjumbe's fave top to wear to a meeting. Burnt it "
}
```
Vous pouvez vous connecter à ce topic sur le broker suivant: 163.172.145.138:9092.

# Applications à implémenter dans le cadre du projet

Vous devrez faire usage d'une technologie de streaming dans ce projet, à choisir dans la dernière partie :).

Chacun de ces traitements devra être fait dans une application séparée (architecture de type
microservices).

Traitement 1 - Extraction de sentiment

Vous devez faire de l'extraction de sentiment sur ces tweets, avec le framework Stanford CoreNLP
(https://stanfordnlp.github.io/CoreNLP/).

L'API en question est edu.stanford.nlp.simple (https://nlp.stanford.edu/nlp/javadoc/javanlp/), à vous
de voir comment l'utiliser dans des transformations autour d'un KStream.

Voici un exemple de code pour extraire le sentiment d'une phrase :

```
import edu.stanford.nlp.simple.*;
```
```
Sentence sent = new Sentence("Lucy is in the sky with diamonds.");
SentimentClass sentiment = sent.sentiment();
sentiment.toString(); // NEGATIVE/NEUTRAL/POSITIVE/VERY_NEGATIVE/VERY_POSITIVE
```

Cette API vous permettra d'extraire des tweets leur sentiment général, que vous écrirez sous format JSON
dans dans un nouveau topic nommé votre-nom_analyzed_tweets.

Traitement 2 - Métriques

Vous devrez ensuite faire des agrégations sur les sentiments par:

```
user avec distribution de sentiment positif/neutre/négatif
par jour avec distribution de sentiment positif/neutre/négatif
par mois avec distribution de sentiment positif/neutre/négatif
par année avec distribution de sentiment positif/neutre/négatif
date, user avec distribution de sentiment positif/neutre/négatif.
Exemple: sur l'année N, par utilisateur, je veux la distribution de sentiments positifs/négatifs
/neutres
```
Enfin, dans ces tweets, certains possèdent des hashtags (mot commençant par #). Vous devez extraire ces
hashtags et extraire les hashtags les plus populaires par jour et par mois.

Chacune de ces aggrégations devra aller dans un topic, avec le nommage de votre choix (contenant votre
nom en préfixe).

Traitement 3 - Dataviz

Enfin, développez une application simple exposant ces métriques via une API REST.

Tips How-To: https://blog.codecentric.de/en/2017/03/interactive-queries-in-apache-kafka-streams/

BONUS : vous pouvez y ajouter une application front affichant des graphes de ces agrégations, en
récupérant les infos dans l'API.

Vous ne devez pas utiliser de base de données, les frameworks de streaming fournissent de quoi faire
tout cela sans base :).

Reminder: préfixez vos stores par votre nom/prénom/nick, pour éviter les problèmes :).

# Technologies possibles (non limité)

```
Natural Language Processing
Stanford CoreNLP: https://stanfordnlp.github.io/CoreNLP/
Streams
Spark Streaming: https://spark.apache.org/streaming/
Kafka Streams: https://kafka.apache.org/documentation/streams/
Akka Streams: https://doc.akka.io/docs/akka/2.5/stream/index.html
REST APIs
Play: https://www.playframework.com/
Akka-HTTP: https://doc.akka.io/docs/akka-http/current/
```

Spring Boot: https://spring.io/guides/gs/rest-service/
Dataviz
D3.js: https://d3js.org/
Highcharts: https://www.highcharts.com/


