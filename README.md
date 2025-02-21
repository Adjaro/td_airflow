## 📥 Étape 1: Cloner le projet

```command
git clone https://github.com/votre-repo/projet.git
```

## 🏗️ Étape 2: Initialiser Airflow

Exécutez la commande suivante pour initialiser la base de données d'Airflow :
```command 
docker-compose up airflow-init
```

---

## ▶️ Étape 3 : Lancer Airflow

Une fois l'initialisation terminée, démarrez Airflow avec :
```command 
docker-compose up
```

Airflow sera accessible à l'adresse :  
🔗 **http://localhost:8080**

Par défaut, les identifiants sont :
- **Utilisateur** : `airflow`
- **Mot de passe** : `airflow`

---

## 🛑 Étape 4 : Arrêter Airflow

Si vous souhaitez arrêter Airflow, utilisez :
```command
docker-compose down
```

Pour tout redémarrer plus tard :
```command
docker-compose up
```

---

## 🔄 Étape 5 : Nettoyage et suppression des containers

Si vous souhaitez **réinitialiser complètement** Airflow, exécutez :
```command
docker-compose down --volumes --remove-orphans
```
Cela supprimera les volumes liés à Airflow.

---

## 🎯 Résumé des commandes importantes
| Commande | Description |
|----------|------------|
| `mkdir dags logs plugins` | Crée les dossiers nécessaires |
| `curl -LO ...` | Télécharge le fichier docker-compose.yaml |
| `Set-Content .env ...` | Configure les variables d'environnement |
| `docker-compose up airflow-init` | Initialise Airflow |
| `docker-compose up` | Démarre Airflow |
| `docker-compose down` | Arrête Airflow |
| `docker-compose down --volumes --remove-orphans` | Supprime tous les volumes Airflow |

---

✨ **Airflow est maintenant installé et opérationnel sous Windows !** 🚀
