## CloudDBGuard ![CloudDBGuard Logo](/logoclouddbguard.PNG)

Column family stores are a special case of NoSQL databases. To achieve data protection while at the same time supporting advanced data
management in these stores, novel cryptographic algorithms like order-preserving and searchable encryption schemes are needed. In this project, several such schemes are implemented and tested with the stores Apache HBase and Apache Cassandra.
### Funding
- Project FamilyGuard (DFG project number WI 4086/2-1)
- Project CloudDBGuard (DFG project number WI 4086/2-2)

## Cite as
-  Lena Wiese, Tim Waage, Michael Brenner. CloudDBGuard: A Framework for encrypted data storage in NoSQL Wide Column Stores. Data and Knowledge Engineering. Elsevier, 2019.
- Daniel Homann and Lena Wiese. Inference Attacks on Fuzzy Searchable Encryption Schemes. Transactions on Data Privacy. Tdp.cat, 2019. 
-  Tim Waage and Lena Wiese. CloudDBGuard: Enabling Sorting and Searching on Encrypted Data in NoSQL Cloud Databases. 20th International Conference on Big Data Analytics and Knowledge Discovery - DaWaK 2018. Springer, 2018.
- Lena Wiese, Daniel Homann, Tim Waage and Michael Brenner. Homomorphe Verschlüsselung für Cloud-Datenbanken: Übersicht und Anforderungsanalyse. Sicherheit 2018, Lecture Notes in Informatics (LNI), pages 221-234. Gesellschaft für Informatik e.V., 2018.
- Tim Waage, Lena Wiese. Property preserving encryption in NoSQL wide column stores. In: Cloud and Trusted Computing (OnTheMove Federated Conferences). Springer, 2017. 27. 
- Christian Göge, Tim Waage, Daniel Homann, Lena Wiese. Improving Fuzzy Searchable Encryption with Direct Bigram Embedding. TrustBus (14th International Conference on Trust, Privacy & Security in Digital Business). Springer, 2017.
- Christian Göge, Tim Waage and Lena Wiese. Implementing a Similarity Searchable Encryption Scheme for Cloud Database Usage. In 29th GI-Workshop Grundlagen von Datenbanken, CEUR Workshop Proceedings. CEUR-WS.org, 2017.
- Tim Waage and Lena Wiese. Implementierung von kryptographischen Sicherheitsverfahren für Cassandra und HBase. (Implementing cryptographic security mechanisms for Cassandra and HBase.) Handbuch der Maschinellen Datenverarbeitung, Springer, 2016.
- Tim Waage und Lena Wiese. Ordnungserhaltende Verschlüsselung in NoSQL Spaltenfamilien-Datenbanken. (Order-preserving encryption in NoSQL column family databases.). In DACH Security. Syssec, 2016.
- Tim Waage, Daniel Homann and Lena Wiese. Practical Application of Order-preserving Encryption inWide Column Stores. In SECRYPT International Conference on Security and Cryptography. SciTePress, 2016.
- Lena Wiese and Tim Waage. Benutzerfreundliche Verschlüsselung für Cloud-Datenbanken (User-friendly encryption for cloud databases). In DACH Security. Syssec, 2015.
- Tim Waage, Ramaninder Singh Jhajj, Lena Wiese. Searchable Encryption in Apache Cassandra. In Foundations and Practice of Security – 8th International Symposium. Lecture Notes in Computer Science 9482, Springer, 2015.
- Tim Waage and Lena Wiese. Benchmarking encrypted data storage in HBase and Cassandra with YCSB. In Foundations and Practice of Security, volume 8930 of Lecture Notes in Computer Science, pages 311–325. Springer, 2014.

## API usage

There are a few simple rules one has to follow when using the API provided by CloudDBGuard:

  * Before database interactions can be performed, the current metadata always has to be loaded upfront by calling the constructor method, described below. In order to keep the metadata and the database contents consistent, the "close" method has to be called after all database related tasks are finished. Doing so saves the current table metadata as well as client side indexes of the used PPE schemes, if required. 
```Java
API api = new API("pathtometadata", "mypassword", true);

... // interactions with the database(s)

api.close();
```
  * CloudDBGuard can only manipulate tables, that have been created using it. Otherwise there would be no metadata available to describe the data structures necessary for the layered encryption, possible data distribution across multiple database instances, etc.
  * In particular, writing as well as querying only works for keyspaces and tables, that have been created using the methods "addKeyspace" and "addTable".

No more rules need to be followed. The user is free to combine arbitrary interactions with the database as he would do without CloudDBGuard.

## ...in more Detail

### Initializing

In order to be able to interact with the databases the API has to be aware of the existing database instances as well as of the data inside them. Thus, it has to load the available keyspace metadata first and initialize the necessary database connections. All of that is taken care of by the API's constructor.

```java
public API(String path, String password, boolean silent);
```

  * path: The file path of the XML metadata file.
  * password: The password that is needed in order to access the keystores managed by this API instance.
  * silent: If set to "false", status output and error messages will be printed to the console. While this helps to see what is going on, it can be a performance bottleneck. If set to "true", no console output will occur.

Assuming the XML metadata file is located in /home/user/mydb.xml and no console output is wanted, the corresponding constructor call would be:

```java
API api = new API("/home/user/mydb.xml", "mypassword", true);
```

Analogous to the initialization, a closing process is required to save the current keyspace metadata state back to the XML file and save client side indexes for future use. All is done by a close method.

```java
public void close();
```

Thus, after all database interactions are done, the necessary corresponding call is

```java
api.close();
```

### Creating Keyspaces/Namespaces

Keyspaces are the highest level of data organisation within the databases. At least one keyspace has to be created in order to house tables. Hence, that is also the first thing that has to be done using the API introduced in this thesis. Consider the following CQL statement: 

```
CREATE KEYSPACE ksn 
WITH REPLICATION = { 
	'class' : 'SimpleStrategy', 
	'replication_factor' : 1 
};
```

As can be seen, it creates a keyspace called "ksn" (short for keyspace name) with the keyspace to be created having certain parameters. When using CloudDBGuard, not only the keyspace name is important, but also what database instances are available to store the tables of the keyspace in the future. One database is mandatory, but arbitrarily more are possible. Thus, the API method for creating keyspaces looks like follows:

```Java
public void addKeyspace(String keyspaceName, String[] dbs, HashMap<String, String> params, String password);
```

  * keyspaceName: the plaintext name of the new keyspace to be created. The API will replace that name with a randomly generated string in every interaction with the database. Thus it will not leak at any point in time.
  * dbs: The database instances available for storing tables of this keyspace. Every string in this array has to be of the form "DatabaseType->IPAddress", e.g. "Cassandra->192.168.2.101".
  * params: Additional parameters that specify, how the new keyspace is handled locally by the database instances. Supported parameters are "replication_class" and "replication_factor". If not specified CloudDBGuard will use the defaults (replication_class = SimpleStrategy and replication_factor = 1 ).
  * password: The password that is needed to access the JCEKS keystore which is used to manage all cryptographic keys required by the PPE schemes that are applied to tables and columns in this keyspace.

The following example shows, how the keyspace "ksn" from the example above could be created, assuming there is an instance of Cassandra and an instance of HBase available for storing table data of this keyspace later on.

```Java
api.addKeyspace("ksn", 
		new String[]{"Cassandra->192.168.0.1" , "HBase->192.168.0.2"},
		new HashMap<String, String>() {{
			put("replication_class", "SimpleStrategy");
			put("replication_factor", "1");
		}}
		"mypassword");
```

### Deleting Keyspaces

Assuming the keyspace created above should be deleted, the necessary CQL query would look like this: 

```
DROP KEYSPACE IF EXISTS ksn;
```

The corresponding API method for deleting tables is: 

```Java
public void dropKeyspace(String keyspaceName);
```

  * keyspaceName: The name of the keyspace to be deleted. Note that all tables existing in this keyspace will be dropped as well.

To delete the keyspace created above, the following API call would be sufficient:

```Java
api.dropKeyspace("ksn");
```
### Creating Tables

After a keyspace has been created, it can be filled with tables. Consider the following CQL query:

```
CREATE TABLE ksn.cars (
	id int PRIMARY KEY,
	model text
); 
```

It creates a very simple table named "cars" within the previously mentioned keyspace "ksn". It has two columns: an integer column "id", which is also the primary key (thus, the row identifier) and a text column "model". To do the same in CloudDBGuard, the following method has to be used:

```Java
public int addTable(String keyspace, String tablename, TableProfile profile, DistributionProfile distribution, String[] columns);
```

  * keyspace: The keyspace in which the new table is to be created.
  * tablename: The plaintext name of the new table. Similar to the keyspace name, it will be replaced by a randomly generated string during every interaction with the database. Thus, it will not leak either.
  * profile: The profile that determines which PPE schemes are used for encrypting the content of the columns in the new table. The available profiles are "TableProfile.FAST", "TableProfile.ALLROUND" and "TableProfile.STORAGEEFFICIENT".
  * distribution: The algorithm, that determines how the table's columns are distributed across the available database instances. Note that the selection of an algorithm only makes any difference, if more than one database instance was provided during the process of creating the table's keyspace. Otherwise no distribution is possible, since only one database is available. The possible options are "DistributionProfile.RANDOM", "DistributionProfile.ROUNDROBIN" and "DistributionProfile.CUSTOM". 
  * columns: An array, that contains a string for every column of the new table. Each of those strings has to have the following format:

[x->][un]encrypted->type->name[->rowid]

  * x: A numerical value in the interval [1 ... number of available database instances]. The column is stored in the xth available database. Specifying this value is only allowed (and makes sense), if "DistributionProfile.CUSTOM" was used and more than one database instance was specified when the table's keyspace was created. 
  * [un]encrypted: By specifying a column as "unencrypted" its contents will be stored without any encryption. In contrast, using the keyword "encrypted" enables the complete onion layer encryption.
  * type: The data type of the column's contents. The available options are "string", "string_set", "integer", "integer_set", "byte" and "byte_set" for text, numerical values and byte blobs and corresponding sets of these types.
  * name: The plaintext name of the column.
  * rowid: The rowid attribute must be set to exactly one column of the table definition in order to specify the row identifier column. If no or more than one column is set to be the row identifier column, the creation of the table will fail. 

While this seems complicated at first glance, it is quite intuitive in practice, as the following two examples will show. In order to create a completely encrypted "cars" table as in the CQL example above, the necessary API call would be:

```Java
api.addTable("ksn", "cars", TableProfile.ALLROUND, DistributionProfile.RANDOM, new String[] {
 	"encrypted->integer->id->rowid",
	"encrypted->string->model"
});
```

To illustrate the creation of a more advanced and actually distributed table, the API call for a table distributed over two database instances looks like:

```Java
api.addTable("company", "employees", TableProfile.ALLROUND,	DistributionProfile.CUSTOM, 
	new String[] {
 		"1->unencrypted->integer->emp-ID->rowid",
		"1->unencrypted->string->firstname",
		"1->unencrypted->string->lastname",
		"1->unencrypted->string->department",
		"2->encrypted->integer->salary"
	}
);
```

As can be seen, the all columns containing information, that is not sensitive, are stored in database 1. Only the salary is stored encrypted in database 2. Note that the row identifier column emp-ID is stored in database 2 automatically as well. Row identifier columns always have to be present in all databases in order to be able to identify (parts of) rows across multiple databases.

### Deleting Tables

Deleting tables is very similar to deleting keyspaces. Assuming the cars table created above is supposed to be dropped, that could be done in CQL issuing:

```
DROP TABLE IF EXISTS ksn.cars;
```

The corresponding API method is:

```Java
public void dropTable(String keyspaceName, String tableName);
```

  * keyspaceName: The keyspace that contains the table to be deleted.
  * tableName: The table to be deleted.

In order to drop the cars table from the example above, one can use:

```Java
api.dropTable("ksn", "cars");
```

### Writing Data

Continuing the example of the cars table introduced above, a CQL statement supposed to fill this table with actual data could look like follows:

```
INSERT INTO ksn.cars (id, model) 
VALUES (12, `Audi');
```

It creates a new row inside the table with the id (and row identifier) 12 and the text "Audi" in the model column. The API method for inserting a row in CloudDBGuard is:

```Java
public void insertRow(String keyspaceName, String tableName, 
			HashMap<String, String> stringData, // "regular" values
			HashMap<String, Long> intData, 
			HashMap<String, byte[]> byteData,  
			HashMap<String, HashSet<String>> stringSetData, // collection types
			HashMap<String, HashSet<Long>> intSetData, 
			HashMap<String, HashSet<ByteBuffer>> byteSetData) 
```

  * keyspaceName: The keyspace of the table, that the new row is written to.
  * tableName: The name of the table, that the new row is written to.
  * stringData/intData/byteData: Maps, that contain the actual values, that are written into the new row, one for each possible data type. The key of the map always contains the name of the column in which the new value is supposed to be written, whereas the value of the map entry contains the actual value. Thus for example: if the numerical value 12 shall be written in the column named id, the map intData has to contain the key-value-pair <"id", 12>. The API then uses the available metadata to find out to which database instance and to what columns the new values have to be written. Note that if columns are encrypted to multiple onion layer columns, one key-value-pair can result in multiple columns.
  * stringSetData, intSetData, byteSetData: The equivalent to stringData/intData/byteData for collection types. Instead of having one single element in a map's value field, sets of values can be inserted at once. Note that sets can only be inserted into columns, that where specified as collection type columns while creating the table previously.

Usually one will not have to insert values of every available type. If a type is not needed, one can use null, instead of passing an empty map, which is the normal case in practice. The example CQL-query from above can be translated into the following API call: 

```Java
api.insertRow("ksn", "cars", 
		new HashMap<String, String>(){{ //stringData
			put("model", "Audi");
		}},
		new HashMap<String, Long>(){{ //intData
			put("id", 12);
		}},
		null, //byteData
		null, //stringSetData
		null, //intSetData
		null //byteSetData
	);
```

### Reading/Querying Data

The API methods introduced so far involved only writing or deleting data, which are operations, that do not return any interesting results. However, a fundamental purpose of databases is of course reading data and thus, getting back exactly specified information. This specification usually comes by executing queries like: 

```
SELECT id, model 
FROM ksn.cars
WHERE ps>100 AND model='BMW';
```

This example is supposed to return all cars with more than 100 PS that where manufactured by BMW. To achieve the same in CloudDBGuard the API method "query" has to be used, which comes with the following signature: 

```Java
public DecryptedResults query(String[] columns, String keyspace, String table, String[] conditions)
```

In contrast to the previously discussed API methods it returns an instance of the class "DecryptedResults" (see below) instead of a primitive data type or void. The remaining parameters are as follows:

  * columns: The columns that are supposed to be part of the result set. In general, all columns that one would write into the SELECT clause of an CQL query should appear here. The column that contains a table's row identifier is always automatically added.
  * keyspace: The plaintext name of keyspace of the table, that the query is executed against.
  * table: The plaintext name of the table, that the query is executed against. To continue the analogy to CQL queries: keyspace and table names would appear in the FROM clause. 
  * conditions: A set of conditions that the resulting rows are supposed to meet. Each element of this set is a string representing a condition in the form: "columnname operator term" with:

  * columnname: The plaintext name of the column that is involved in this condition.
  * operator: The operator used to define the condition. The following operators are allowed:

```
  =: equal (makes use of the DET layer)
  >: greater than (makes use of the OPE layer)
  <: less than (makes use of the OPE layer)
  #: includes (makes use of the SE layer, only for text columns)
```

  Note the difference between = and # when it comes to text values. While = checks for equality of complete strings, # can be used to search for single words within these strings. For example working with the condition "model=BMW" would return only rows, where model exactly matches the string "BMW", whereas using the "#" operator would also return rows like "BMW 320d", "new great BMW car", etc. Thus the # operator works similar to SQL's LIKE %term% operator.

  Note further, that the # operator is only available in encrypted text columns, since it realizes its functionality utilizing the capabilities of SE. 

  * term: The term used to define the condition.

Thus, the call for the above presented example looks as follows:

```Java
DecryptedResults results = api.query
	(new String[]{"id", "model", "ps"},	// SELECT
	"ksn", "cars",				// FROM
	new String[]{"ps>100", "model=BMW"});	// WHERE, also # possible
															// for getting everything
															// that includes "BMW"
```

## Methods for Decrypted Result Sets

When a query is issued using the provided API method, an instance of the class "DecryptedResults" is returned. If the (logical) table, that the query was executed against, was spread across multiple databases, it joins the individual column data from all of the corresponding (physical) tables (only from rows that fulfill the query's conditions, of course). It also provides access to columns that have been stored originally in unencrypted form.

Depending on the expected result type the user can access individual decrypted values by calling one of these methods:

```Java
public String getStringValue(byte[] id, String column);
public int getIntValue(byte[] id, String column);
public byte[] getByteValue(byte[] id, String column);
public Set<String> getStringSetValue(byte[] id, String column);
public Set<Integer> getIntSetValue(byte[] id, String column);
public Set<byte[]> getByteSetValue(byte[] id, String column);
```

In all of these methods the parameter id is the row identifier of the row that is about to be accessed, whereas column is the name of the column that is about to be accessed. Of course, only columns that were specified in the columns-parameter of the corresponding query call are available here, because only those have been selected to be in the result set in the first place.

The "DecryptedResults" class also provides two more auxiliary methods for handling a query's result set:

```Java
public int getSize();
public void print(int numberOfRowsToPrint);
```

As their names suggest getSize() delivers the total numbers of rows of the result set and print(int numberOfRowsToPrint) prints the specified number of rows to the standard console. 
