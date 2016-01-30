# DataBreakers Keboola writer

This library provides PHP implementation of Keboola writer to DataBreakers DataAPI.

## Installation

Clone the git repository and use Composer to install dependencies:

```sh
$ git clone github/... .
$ composer install
```

## Running

Run the script `php src/run.php --data=/data` with path to data parameter.
In Keboola enviroment the data path is `/data`.

## Configuration

Script needs a standard Keboola configuration file placed in `/data/configuration.json` or `/data/configuration.yml` with following custom parameters.

```
{
    "parameters": {
        "databreakers_account": DATABREAKERS_ACCOUNT_ID,
        "databreakers_secret_key": DATABREAKERS_API_SECRET,
        "delete_old_data": true|false
    },
}
```

You need to get `DATABREAKERS_ACCOUNT_ID` and `DATABREAKERS_API_SECRET` first to use the writer. Please contact keboola@databreakers.com if you don't have your account yet.
`delete_old_data` signifies if all actual data in your DataBreakers account will be deleted or you want to append new data to account actual dataset.

## Entities

Writer operates with three entites: *items*, *users*, *interactions*. **Item** entities are... **User** entities are... **Interaction** entites are...

To upload a set of entites into DataBreakers API, you have to create *csv* exports named accordingly to entity type: `users.csv`, `items.csv`, `interactions.csv`.

## Exports requirements

`items.csv` has to have **PK defined on one column**. Then it can have as many attributes (columns) as you need. Name of every column (except PK) will correspod to *item*'s attributes of *items* in DataBreakers dataset. This file is **not required**, you can use it to add explicit attributes to *items*.

`users.csv` has to have **PK defined on one column**. Then it can have as many attributes (columns) as you need. Name of every column (except PK) will correspod to *user*'s attributes of *users* in DataBreakers dataset. This file is **not required**, you can use it to add explicit attributes to *users*.

`interactions.csv` has to have exactly **4 columns** named as `user_id`, `item_id`, `timestamp`, `interaction_id` and **PK** on all these four columns. Default *interaction_id* set is defined as `[Detail view, Purchase, Recommendation, Dislike, Like, Bookmark]`. You can have your own extended set if you need, just contact support. This file **is required**. It will create empty *user* or *item* entity if *user_id* or *item_id* is not presented in your dataset.

-----

[DataBreakers](https://databreakers.com) – we are your data sense