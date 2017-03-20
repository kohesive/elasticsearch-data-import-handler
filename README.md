# Elasticsearch Data Import Handler

A data import handler for Elasticsearch

* Simple
* Powerful
* Use SQL statements that can span multiple databases, text files, and Elasticsearch indexes
* Process full load and incremental updates
* Output columnar and structured JSON to Elasticsearch

Running is simple.  With Java 8 installed, download a release and then run it:

```
kohesive-es-dih <configFile.conf>
```

The config file is in [HOCON format](https://github.com/typesafehub/config/blob/master/HOCON.md) which is a relaxed 
version of JSON and allows [multi-line-string](https://github.com/typesafehub/config/blob/master/HOCON.md#multi-line-strings) 
which is very useful when writing larger SQL statements.

The configuration file follows this format:

```hocon
{
  "sources": {
    "elasticsearch": [ ],
    "jdbc": [ ],
    "filesystem": [ ]
  },
  "prepStatements": [ ],
  "importSteps": [ ]
}
```

First you should provide 1 or more `sources`.  Each source becomes a temporary table in a unified catalog of tables from 
which you can query and join together.

Optionally, you can include preparatory steps in `prepStatements` which are usually additional  temporary tables 
that may be shared by the later import steps.  The `prepStatements` execute in order and **do not** do any date range 
substitution.

Lastly you specify `importSteps` which are queries that use any of the `sources` and temporary tables created in 
`prepStatements`; where the results of each SQL query is pushed into an Elasticsearch index.  

**Tip:** The SQL used is anything available to [Apache Spark SQL](https://docs.databricks.com/spark/latest/spark-sql/index.html)

### Let's start with an example, 
...of loading 1 table from MySQL into Elasticsearch:

```hocon
{
  "sources": {
    "jdbc": [
      {
        "jdbcUrl": "jdbc:mysql://localhost/test?useSSL=false",
        "driverClass": "com.mysql.jdbc.Driver",
        "defaultSchema": "test",
        "auth": {
          "username": "myusername",
          "password": "mypass"
        },
        "driverJars": [
          "/Users/myUserName/Downloads/mysql-connector-java-5.1.41/mysql-connector-java-5.1.41-bin.jar"
        ],
        "tables": [
          {
            "sparkTable": "Users",
            "sourceTable": "UserEntities"
          }
        ]
      }
    ]
  },
  "importSteps": [
    {
      "description": "Data loaders for base data sets",
      "targetElasticsearch": {
        "nodes": [
          "localhost:9200"
        ],
        "settings": {
          "es.index.auto.create": true
        }
      },
      "statements": [
        {
          "id": "Q4499_1233",
          "description": "Load User data into ES",
          "indexName": "aa-test-user",
          "type": "user",
          "settings": {
            "es.mapping.id": "guid"
          },
          "sqlQuery": """
                SELECT guid, first_name, last_name, organization 
                  FROM Users
                 WHERE dtUpdated BETWEEN '{lastRun}' and '{thisRun}'
          """
        }
      ]
    }
  ]
}
```

You will see that the JDBC source is provided, which must include the JDBC driver for the database, connection information,
and a mapping from the original `sourceTable` database table to the temporary table `sparkTable` that will be used in 
later SQL queries.  The name `sparkTable` is used because this system runs an embedded Apache Spark, and is creating Spark SQL
tables from the configuration.  

Since this process runs in Apache Spark, there might be additional options you wish to set when 
the data is loaded.  For advanced users who know what these are, you can add `settings` map at the the `jdbc` connection
level to apply to all tables within that connection, or at the per-`tables` level of the configuration to apply to only
one table.  

The `importSteps` are a collection of target Elasticsearch clusters and one or more SQL statements for each.  Each statement
must have a unique `id` so that state can be tracked, changing or removing the `id` will result in a full data load running
for a given query.  

Notice that the SQL statement includes the use of the `{lastRun}` and `{thisRun}` macros.  These will substitute the current
date/time into the SQL as a SQL Date formated string.  The granularity is SECONDS, and the local time zone of the data 
import processor is used.  Also, be sure to put the date macros inside quotes.

The `type` field is the target type within the Elasticsearch `indexName` for the documents.  You can use either the 
literal type name, or include a macro of `{fieldName}` where `fieldName` is one of the fields in the SQL result set.  

The `settings` object for Elasticsearch are important, and the most common basic settings you may wish to set (at either 
the connection or statement level) are:

|Setting|Description|
|-------|-----------|
|es.index.auto.create|Whether to auto-create the target index if it doesn't already exist|
|es.mapping.id|Which field in the SQL results should be used as the ID for the document (if absent, autogenerated ID's are used)|
|es.ingest.pipeline|Which ingest pipeline should be used to pre-process incoming records into Elasticsearch|

**Tip:** For advanced use cases, please see documentation for [Elasticsearch-Spark settings](https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html).

### Let's try a more complex example,
...of joining a csv text file to the SQL statement

```hocon
{
  "sources": {
    "jdbc": [
      {
         # ... same as previous example
      }
    ],
     "filesystem": [
          {
            "directory": "/Users/jminard",
            "tables": [
              {
                "sparkTable": "UserEmotions",
                "format": "csv",
                "filespecs": [
                  "test.csv"
                ],
                "settings": {
                  "header": true,
                  "inferSchema": true
                }
              }
            ]
          }
        ]
  },
  "importSteps": [
    {
      "description": "Data loaders for base data sets",
      "targetElasticsearch": {
        # ... same as previous example
      },
      "statements": [
        {
          "id": "Q4499_1233",
          "description": "Load User data with merged Emotions into ES",
          "indexName": "aa-test-user",
          "type": "user",
          "settings": {
            "es.mapping.id": "guid"
          },
          "sqlQuery": """
                SELECT u.guid, u.first_name, u.last_name, u.organization, ue.emotion 
                  FROM Users AS u LEFT OUTER JOIN UserEmotions AS ue ON (u.guid = ue.guid)
                 WHERE u.dtUpdated BETWEEN '{lastRun}' and '{thisRun}'
          """
        }
      ]
    }
  ]
}
```

We have changed the configuration adding the file source.  Here the `directory` must exist and then `filespec` is a list
of specific filenames, or wildcards.  For example `["test1.csv", "test2.csv"]` or `["test*.csv"]` are both valid.  Here again
you may see `settings` maps appearing at the filesystem directory level or for each table.  

The format may be `csv`, `json`, tab delimited, or any other import file format supported by the default Apache Spark 
distribution.  Some settings you might find useful for `csv` include:

|Setting|Description|
|-------|-----------|
|header|Whether a header line is present or not (true/false)|
|delimiter|What delimiter between the fields, default `,`|
|quote|If fields are quoted, what character is used for quoting, default `"`|
|escape|If escaping of characters is needed, what character is used, default `\`|
|charset|If the file is not `UTF-8`, what charset is it?|
|inferSchema|The system can infer datatypes by scanning all the data first, then loading in a second pass (true/false)|
|nullValue|This string can replace any null values, otherwise they are truly null|
|dateFormat|specifies a date format for recognizing and parsing date fields (follows [SimpleDateFormat](https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html))|

**Tip:** For other use cases, read all [CSV Options](https://github.com/databricks/spark-csv)

### And if you want an example with an Elasticsearch source
...here is the additional part of the configuration:

```hocon
{
  "sources": {
    "elasticsearch": [
      {
        "nodes": [
          "localhost:9200"
        ],
        "tables": [
          {
            "sparkTable": "SearchDocuments",
            "indexName": "document-contents-2017",
            "type": "rawdoc",
            "esQuery": {
              "match_all": {}
            }
          }
        ]
      }
    ],
    # ...
}
```

Now I have the Elasticsearch index acting as a table.  Like before, `settings` maybe applied at the connection or `tables` level
if you have special needs.  And please note that the `esQuery` contains the first level of query filtering, and if absent 
defaults to a `match_all` query.  You can write the query as above, or you can also include the top level `"query": { ... }"` 
element surrounding the query.  

### Let's go for it, here's a bigger example
...showing an import that generates nested JSON and also a preparatory step creating temporary tables.

We will use the same source tables as the previous examples and imagine that we now have `Orgs` and `OrgMembers` tables.
We will create a final list of `Users` with their organizations nested inside each.  We also will update the users if
any of the related tables change.  This might cause an explosion of updates, so be careful with your planning about how
you use the `{lastRun}` and `{thisRun}` values.

```hocon
{
  "sources": {
    # ... same as above but we have added `Orgs` and `OrgMembers`
  },
  "prepStatements": [
    {
      "description": "Create views of only the active users",
      "sqlQuery":  """
            CREATE TEMPORARY VIEW ActiveUsers AS
                SELECT * FROM Users WHERE isActive = 1
      """
    },
    {
      "description": "Create views of only the active orgs",
      "sqlQuery":  """
            CREATE TEMPORARY VIEW ActiveOrgs AS
                SELECT * FROM Orgs WHERE isActive = 1
      """
    }
  ],
  "importSteps": [
    {
      "description": "Data loaders for base data sets",
      "targetElasticsearch": {
        "nodes": [
          "localhost:9200"
        ],
        "settings": {
          "es.index.auto.create": true
        }
      },
      "statements": [
        {
          "id": "X9A90Z_1",
          "description": "Load denormalized User + Org Memberships into ES",
          "indexName": "aa-test-user",
          "type": "{docType}",
          "settings": {
            "es.mapping.id": "guid"
          },
          "sqlQuery": """
                WITH
                    orgMembers AS
                    (
                       SELECT our.guid AS roleOrgGuid, our.userGuid AS roleUserGuid, our.orgUserRoleType AS roleType, our.dtUpdated AS dtRoleUpdated,
                              oe.displayName AS orgDisplayName, oe.dtUpdated AS dtOrgUpdated
                         FROM ActiveOrgs AS oe JOIN OrgMembers AS our ON (our.orgGuid = oe.guid)
                    ),
                    userWithOrg AS (
                       SELECT ue.guid, struct(ue.*) AS user, struct(om.*) AS orgMembership
                         FROM ActiveUsers AS ue LEFT OUTER JOIN orgMembers AS om ON (om.roleUserGuid = ue.guid)
                    ),
                    modifiedUserData AS (
                        SELECT guid, first(user) as user, collect_list(orgMembership) AS orgMemberships
                                 FROM userWithOrg AS ue
                                WHERE user.dtUpdated between "{lastRun}" AND "{thisRun}" OR
                                      orgMembership.dtRoleUpdated between  "{lastRun}" AND "{thisRun}" OR
                                      orgMembership.dtOrgUpdated between  "{lastRun}" AND "{thisRun}"
                             GROUP BY guid
                    ),
                    usersWithEmotions AS (
                        SELECT mu.*, em.emotion FROM modifiedUserData AS mu LEFT OUTER JOIN UserEmotions AS em ON (mu.guid = em.guid)
                    )
                    SELECT user.accountType as docType, user.guid,
                           user.identity, user.displayName, user.contactEmail, user.avatarUrl, user.gravatarEmail, user.blurb,
                           user.location, user.defaultTraitPrivacyType, user.companyName, user.isActive, user.isHeadless,
                           emotion, user.dtCreated, user.dtUpdated, orgMemberships FROM usersWithEmotions
          """
        }
      ]
    }
  ]
}
```

Ok, that was a bit much.  But here is some research you can do.  In the [SQL Functions API Reference](https://spark.apache.org/docs/2.1.0/api/java/org/apache/spark/sql/functions.html) 
you will find the `collect_list` and `struct` functions.  They are used above to create structured results, like nested JSON.  The `struct()`
function is combining columns into an object, and the `collect_list` is aggregating the objects into an array.  

We are also using the `guid` from the result set as the Elasticsearch document `_id`, and we have a literal field coming back
in the documents that we are using via the `type` setting as a macro `{docType}` pointing at that field from the result set.
This is handy if different documents in the results will have different types. 

So at the end we have a result that looks like:

```hocon
{
  "docType": "...",
  "guid": "...",
  "identity": "...",
  # ...
  "orgMemberships": [
     {
         "roleOrgGuid": "...",
         "orgDisplayName": "...",
         # ...
     },
     {
         "roleOrgGuid": "...",
         "orgDisplayName": "...",
         # ...
     }
  ]
}
```

### SQL Reference:

The data import handler uses Spark SQL, and you can read the [Spark SQL Reference](https://docs.databricks.com/spark/latest/spark-sql/index.html) 
for full details on the supported SQL.  

A list of [SQL Functions](https://spark.apache.org/docs/2.1.0/api/java/org/apache/spark/sql/functions.html) 
is available in raw API docs.  (_TODO: find better reference_)

### State Management:

State for the `lastRun` value is per-statement and stored in the target Elasticsearch cluster for that statement.  An index
will be created called `.kohesive-dih-state` which stores the last run state, a lock for current running statements, and
a log of all previous runs (success and failures).  You should inspect this log if you wish to monitor the results of
runs.


### Parallelism

By default, the data import handler is running locally with `Processors-1` parallelism.  You can set the following 
top-level configuration setting to change the parallelism:

```
{
    "sparkMaster": "local[N]",
    "sources": { 
        # ... 
    } 
}
```

Where `N` is the number of partitions you wish to run.  Since this is the Spark master setting, some people might try
connecting to a Spark cluster using the setting.  It just might work! 

### Memory Issues

If you run out of memory you can set the Java VM parameters via the `KOHESIVE_ES_DIH_OPTS` environment variable before
running the `kohesive-es-dih` script.  For example, to set it to 2G: `-Xmx2g` 

### TODOs

* logging the processed count for each statement, currently if the SQL is valid but produces 0 results, you have no idea.
* time zone offsets for any source SQL server (currently substitution times into SQL are local timezone of client)
* allow date patterns in target index names
* allow alias swapping at end of loading to a new index (add verifiers before swap?)
* streaming from MySQL replication
* other forms of streaming?
* testing and docs around using Spark Clusters in different forms (stand-alone, YARN, ...)