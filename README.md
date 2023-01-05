# Hitching Spots Ingestion

Downloads hitching spots from HitchWiki and writes them to the Hitchhike App database.

Low quality data is filtered out. Rows with a timestamp earlier than the oldest timestamp in firestore are automatically ignored.

# Usage

```shell
npm start
```
