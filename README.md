# Labo 08 – Saga chorégraphiée, CQRS avec event broker, patron Outbox

<img src="https://upload.wikimedia.org/wikipedia/commons/2/2a/Ets_quebec_logo.png" width="250">    
ÉTS - LOG430 - Architecture logicielle - Chargé de laboratoire : Gabriel C. Ullmann, Automne 2025

## 🎯 Objectifs d'apprentissage
- Comprendre le fonctionnement d'une Saga chorégraphiée implémentée dans multiples microservices en utilisant Kafka en tant qu’event broker
- Comprendre la différence entre les patrons Saga orchestrée (labo 6) et chorégraphiée (labo 8)
- Observer comment une architecture event-driven travaille ensemble avec les concepts CQRS
- Utiliser le patron Outbox pour augmenter la tolérance aux pannes dans une application
- Implémenter des event handlers qui réagissent aux événements et déclenchent des actions compensatoires

## ⚙️ Setup
L'architecture de notre application Store Manager a beaucoup changé lors de dernières labos. Voici une petite récapitulation :
| Labo | Architecture   | Communication entre services | Compensation des opérations échouées |
|------|----------------|--------------------------------------------------------------------------------|-------------------------------------|
| 00-02 | Monolithique  | Il n'y a pas de services, toutes les fonctionnalités sont dans le même monolithe | Non  |
| 03-04 | Monolithique avec API REST  | Il n'y a pas de services, toutes les fonctionnalités sont dans le même monolithe | Non  |
| 05    | Microservices | Synchrone, via requêtes HTTP directement entre les services | Non |
| 06    | Microservices | Synchrone, via requêtes HTTP à un orchestrateur qui appelle chaque service au bon moment  | Oui  |
| 07    | Microservices | Asynchrone, via requêtes HTTP à un broker Kafka qui travaille comme intermédiaire entre les services  | Non |
| 08    | Microservices | Asynchrone, via événements Kafka avec chorégraphie (chaque service réagit aux événements, sans exchange de requêtes HTTP directe entre eux) | Oui |

Dans le labo 08, nous allons combiner les approches des labos 06 et 07 pour créer une application Saga chorégraphiée. Pour supporter notre implémentation, nous utiliserons les patrons CQRS (en place depuis le labo 2) et Outbox. Contrairement au labo 06 où un orchestrateur central coordonnait tous les services, ici chaque service réagit de manière autonome aux événements publiés dans Kafka par des autres services. L'analogie avec une danse n'est pas une coïncidence : chaque mouvement d'une application déclenche les mouvements en réponse dans les autres services. 

Pour en savoir plus sur l'architecture et les décisions de conception, veuillez consulter le document d'architecture sur `/docs/arc42/docs.md` et l'ADR sur `/docs/adr/adr001.md` avant de commencer les activités.

> 📝 **NOTE** : Dans une vraie application, nous pouvons utiliser un cluster de brokers Kafka distribués. Ça veut dire que même si un broker cesse de fonctionner, un autre broker sur un autre serveur peut continuer le travail avec les mêmes clients. Par simplicité, dans ce labo, on ne va utiliser qu'un seul broker (single-cluster).

### 1. Clonez les dépôts
Vous allez travailler sur **deux dépôts** lors de ce labo : Store Manager (`log430-labo8`) et Payments API (`log430-a25-labo5-payment`). Créez vos propres dépôts à partir des dépôts gabarit (template). Vous pouvez modifier la visibilité pour le rendre privé si vous le souhaitez :
```bash
git clone https://github.com/[votrenom]/log430-a25-labo5-payment
git clone https://github.com/[votrenom]/log430-labo8
cd log430-labo8
```

Ensuite, veuillez faire les étapes de setup suivantes pour les **deux dépôts**.

### 2. Créez un fichier .env
Créez un fichier .env basé sur .env.example. Suivez les mêmes étapes que pour les derniers laboratoires.

### 3. Créez un réseau Docker
Utilisez le même réseau dans le `docker-compose.yml` des deux dépôts. Si vous ne l'avez pas encore créé, exécutez :
```bash
docker network create labo08-network
```

### 4. Préparez l'environnement de développement
Démarrez les conteneurs de TOUS les services. Suivez les mêmes étapes que pour les derniers laboratoires.
```bash
docker compose build
docker compose up -d
```

## 🧪 Activités pratiques

> ⚠️ **ATTENTION** : Dans ce laboratoire, nous allons analyser et modifier des fichiers dans les dépôts `log430-labo8` (`store_manager`) et `log430-a25-labo5-payment` (`payments_api`). Veuillez faire attention à l'énoncé de chaque activité afin de savoir quel dépôt utiliser.

### 1. Analysez l'architecture actuelle

Dans le dépôt `log430-labo8`, regardez la section 5 du document arc42 (`/docs/arc42/docs.md`). Dans cette section, vous allez voir les diagrammes qui expliquent les classes qui ont les responsabilités event-driven (tels que `Handler`, `HandlerRegistry`, `OrderEventProducer`, etc.), très similaire à ce que vous avez utilisé pendant le labo 7. La seule différence entre le labo 8 et le labo 7 c'est qu'ici on a plus de Handlers, et on travaille avec des commandes au lieu des utilisateurs. Vous pouvez les voir dans les répertoires `handlers` dans chaque domaine de l'application (`orders`, `stocks` et `payments`).

Cependant, si on compare le labo 8 avec le labo 6, il y a bien d'autres différences. Répondez aux questions :

> 💡 **Question 1** : Comment on faisait pour passer d'un état à l'autre dans la saga dans le labo 6, et comment on le fait ici? Est-ce que le contrôle de transition est fait par le même structure dans le code? Illustrez votre réponse avec des captures d'écran ou extraits de code.

> 💡 **Question 2** : Sur la relation entre nos Handlers et le patron CQRS : pensez-vous qu'ils utilisent plus souvent les Commands ou les Queries? Est-ce qu'on tient l'état des Queries à jour par rapport aux changements d'état causés par les Commands? Illustrez votre réponse avec des captures d'écran ou extraits de code.

### 2. Observez le service en action
Utilisez Postman pour appeler l'endpoint `POST /orders`. Si tout se passe bien, vous verrez les message suivantes dans les logs à Store Manager via Docker Desktop :
```bash
POST "/orders HTTP/1.1" 201 -
OrderConsumer - DEBUG - Evenement : OrderCreated
Handler - DEBUG - payment_link=no-link
OrderConsumer - DEBUG - Evenement : StockDecreased
Handler - DEBUG - payment_link=no-link
OrderConsumer - DEBUG - Evenement : PaymentCreated
Handler - DEBUG - payment_link=todo-add-payment-link-here
OrderConsumer - DEBUG - Evenement : SagaCompleted
Handler - INFO - Saga terminée avec succès ! Votre order_id = 1. Votre payment_link = 'todo-add-payment-link-here'.
2025-11-09 22:19:42 - Handler - INFO - [données de la commande]
```
Ici, nous pouvons observer que les Handlers sont déjà en place, qu'ils s'appellent déjà les uns les autres et suivent une chorégraphie. Maintenant, vous devez leur faire exécuter les opérations nécessaires.

> 📝 **NOTE** : Les logs peuvent parfois mettre entre 15 et 30 secondes à s'afficher en raison des délais de démarrage des services associés au Store Manager et de la file d'attente des événements dans Kafka. Soyez patient 🙂. Si vous préférez, vous pouvez utiliser le paramètre `log_to_file=True` dans une instance du Logger pour imprimer les logs dans un fichier supplémentaire dans Docker Desktop.

### 3. Implémentez les Handlers de stock
Voici une résumé des Handlers de commandes et stocks utilisés pendant la Saga dans le dépôt `log430-labo8`. Chaque classe Handler représente une transition d'état, soit pour faire une opération ou pour faire sa compensation.
| **Domaine**  | **Handler**           | **C'est prêt**  |
|--------------|-----------------------|-----------------|
| Orders       | OrderCreated          | Oui             |
|              | OrderCreationFailed   | Oui             |
|              | OrderCancelled        | Oui             |
|              | SagaCompleted         | Oui             |
| Stocks       | StockIncreased        | **Non**         |
|              | StockDecreaseFailed   | **Non**         |
|              | StockDecreased        | **Non**         |

Maintenant, c'est à vous d'implémenter les Handlers qui ne sont pas encore implémentés. Utilisez **la même logique** et les mêmes méthodes que vous avez utilisées pendant le **labo 6**. Si jamais vous n'êtes pas certain d'où mettre le code ou à quel état faire la transition, consultez le diagramme de machine à états dans le document arc42. Les commentaires `TODO` sont également dans le code pour vous donner une idée d'où mettre le code.

Pour exécuter votre implémentation, utilisez Postman pour appeler l'endpoint `POST /orders` et déclencher la saga. C'est la méthode `add_order` dans `src/orders/commands/write_order.py` qui déclenche le premier événement, et les autres Handlers suivent la chorégraphie. 

Si jamais vous trouvez un problème, utilisez les mêmes astuces de débogage que nous avons discuté pendant les derniers labos.

### 4. Préparez le Payments API à recevoir des événements
Avant d'implémenter les Handlers de paiement, vous aurez besoin de créer aussi un consommateur Kafka dans `log430-a25-labo5-payment` pour écouter l'événement `StockDecreased` et un producteur Kafka pour émettre les événements `PaymentCreated` ou `PaymentCreationFailed`, selon le besoin. Pour faire ça dans `log430-a25-labo5-payment`, vous devez:
- Ajouter la dépendance `kafka-python` sur `requirements.txt`. **Reconstruisez vos conteneurs** pour installer les dépendances.
- Créer la structure qui est nécessaire pour permettre au consommateur d'appeler des Handlers (ex. `HandlerRegistry`). Copiez les classes pertinents **qui existent déjà** dans `log430-labo8` à `log430-a25-labo5-payment`.
- Utiliser les consommateurs et producteurs, tels que nous avons vu pendant les labos 7 et 8 :
```python
import config
from event_management.handler_registry import HandlerRegistry
from stocks.handlers.stock_decreased_handler import StockDecreasedHandler
from orders.queries.order_event_consumer import OrderEventConsumer

registry = HandlerRegistry()
registry.register(StockDecreasedHandler())
consumer_service = OrderEventConsumer(
    bootstrap_servers=config.KAFKA_HOST,
    topic=config.KAFKA_TOPIC,
    group_id=config.KAFKA_GROUP_ID,
    registry=registry
)
consumer_service.start()
```

### 5. Implémentez les Handlers de paiement
Implémentez les Handlers restants.
| **Domaine**  | **Handler**           | **C'est prêt**  |
|--------------|-----------------------|-----------------|
| Payments     | PaymentCreated        | **Non**         |
|              | PaymentCreationFailed | **Non**         |

### 6. Implémentez le patron Outbox
La saga chorégraphiée a comme avantage le fait que le déclenchement des événements est distribué. Ça veut dire qu'un service n'a pas besoin de savoir quelle est l'adresse des autres services, il faut simplement connaître Kafka. De plus, même si un des services est hors ligne, ça n'arrête pas le flux des événements de la saga complètement, comme c'était le cas avec l'orchestrateur. Cependant, nous restons avec un problème : par exemple, si avant de créer le paiement, notre serveur est éteint ou l'application est arrêtée de manière forcée, une ou plusieurs commandes vont rester dans la base de données sans `payment_link`. Comme dans ce cas l'application est arrêtée soudainement, il n'y a pas assez de temps pour faire la compensation, et ainsi la commande devient incohérente.

Le patron [Outbox](https://wipiec.digitalheritage.me/index.php/wipiecjournal/article/view/101) a été créé pour résoudre ce type de problème. Dans `log430-labo8`, dans le Handler `StockDecreased`, au lieu de demander le paiement tout de suite, nous allons enregistrer cette demande dans la table `Outbox` et ensuite appeler la classe `OutboxProcessor`. La persistance des événements et l'utilisation du `OutboxProcessor` aux bons moments nous permettra d'y accéder une fois que l'application a redémarré et d'allez jusqu'au bout dans la saga.

![Outbox Pattern](docs/arc42/outbox.png)
Figure 1 - Diagramme de séquence d'une application q'utilise le patron Outbox.

### 6.1 Changez la classe StockDecreasedHandler
Changez la méthode `handle` dans `StockDecreasedHandler` pour enregistrer les donées de la commande dans la table `Outbox` et ensuite appeler `OutboxProcessor`, qui appellera l'API Payments de manière synchrone.
```python
    session = get_sqlalchemy_session()
    try: 
        new_outbox_item = Outbox(order_id=event_data['order_id'], 
                                user_id=event_data['user_id'], 
                                total_amount=event_data['total_amount'],
                                order_items=event_data['order_items'])
        session.add(new_outbox_item)
        session.flush() 
        session.commit()
        OutboxProcessor().run(new_outbox_item)
    except Exception as e:
        session.rollback()
        self.logger.debug("La création d'une transaction de paiement a échoué : " + str(e))
        event_data['event'] = "PaymentCreationFailed"
        event_data['error'] = str(e)
        OrderEventProducer().get_instance().send(config.KAFKA_TOPIC, value=event_data)
    finally:
        session.close()
```

### 6.2 Changez le point d'entrée du service Store Manager
Changez votre fichier `store_manager.py` pour appeler `OutboxProcessor` à chaque initialisation du Store Manager. Veuillez placer ce code avant la déclaration du premier endpoint Flask.

```python
from payments.outbox_processor import OutboxProcessor

# il faut éxécuter le processeur seulement 1 fois à chaque initialisation
is_outbox_processor_running = False
if not is_outbox_processor_running:
   OutboxProcessor().run()
   is_outbox_processor_running = True
```

> 💡 **Question 3** : Est-ce qu'une architecture Saga orchestrée pourrait aussi bénéficier de l'utilisation du patron Outbox, ou c'est un bénéfice exclusif de la saga chorégraphiée? Justifiez votre réponse avec un diagramme ou en faisant des références aux classes, modules et méthodes dans le code.

> 💡 **Question 4** : Qu'est-ce qui arriverait si notre application s'arrête avant la création de l'enregistrement dans la table `Outbox`? Comment on pourrait améliorer notre implémentation pour résoudre ce problème? Justifiez votre réponse avec un diagramme ou en faisant des références aux classes, modules et méthodes dans le code.

## ✅ Correction des activités
Le fichier `src/tests/test_saga.py` contient des tests unitaires pour vérifier si la saga a éxécuté correctment. Executez ces tests **via Docker Desktop** en utilisant l'onglet **Exec** :
```bash
python3 -m pytest
```

> 📝 **NOTE** : Vous pouvez également exécuter les tests localement, mais cela est un peu plus compliqué car vous devrez vous assurer que les hostnames de votre fichier .env font référence à `localhost`, et que tous les ports nécessaires sont ouverts. Je vous recommande donc vivement de l'exécuter dans Docker.

Si tous les tests passent ✅, vos implémentations sont correctes.

## 📦 Livrables

- Un fichier `.zip` contenant l'intégralité du code source du projet Labo 08.
- Une vidéo expliquant les principales observations, décisions et défis/difficultés/problèmes rencontrés durant l'étape 3 (labos 6-8).
    - Exigences : Maximum 5 minutes, format .mp4 ou .webm.
    - Veuillez utiliser un outil tel que Handbrake pour compresser la vidéo si elle dépasse 20 Mo.
- Un rapport en `.pdf` répondant aux questions présentées dans ce document. Il est obligatoire d'illustrer vos réponses avec du code ou des captures d'écran/terminal
