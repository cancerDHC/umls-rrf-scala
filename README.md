# umls-rrf-scala
A very basic library for parsing files in the UMLS RRF format

## How to use
This program expects the `META` directory from the
[NLM UMLS download](https://www.nlm.nih.gov/research/umls/index.html)
to be in the directory where the program is executed. The `--rrf-dir`
command line argument can also be used to point to the `META` directory
in another directory.

```console
$ sbt "run --from-source SNOMEDCT_US --to-source NCI -o mappings.txt"
[info] Loading settings for project global-plugins from metals.sbt ...
[info] Loading global plugins from /Users/gaurav/.sbt/1.0/plugins
[info] Loading settings for project umls-rrf-scala-build from plugins.sbt ...
[info] Loading project definition from /Users/gaurav/Development/umls-rrf-scala/project
[info] Loading settings for project umls-rrf-scala from build.sbt ...
[info] Set current project to umls-rrf (in build file:/Users/gaurav/Development/umls-rrf-scala/)
[info] Compiling 1 Scala source to /Users/gaurav/Development/umls-rrf-scala/target/scala-2.13/classes ...
[info] running (fork) org.renci.umls.CodeMapper --from-source SNOMEDCT_US --to-source NCI -o mappings.txt
[info] 15:32:38.794 [main] INFO  org.renci.umls.CodeMapper$ - Loaded directory for release: # Release Metadata
[info] #Tue Oct 02 03:11:52 EDT 2018
[info] umls.release.date=20180801
[info] umls.release.name=201808
[info] mmsys.build.date=2018_09_17_23_18_10
[info] mmsys.version=MMSYS-2016AA-20160329
[info] nlm.build.date=2018_10_01_14_48_01
[info] umls.release.description=Base Release for 201808
[info] 15:32:38.802 [main] INFO  org.renci.umls.CodeMapper$ - Using SQLite backend: org.apache.commons.dbcp2.DriverManagerConnectionFactory@434a63ab
[info] 15:32:43.298 [main] INFO  org.renci.umls.db.DbConcepts - Concept table MRCONSO_bda810d0c047e368385a441642703331581153cd7fe728fdc256a3e3960f1c40 has 6857637 rows.
[info] 15:32:43.725 [main] INFO  org.renci.umls.db.DbConcepts - Loading halfmaps for SNOMEDCT_US
[info] 15:32:44.930 [main] INFO  org.renci.umls.db.DbConcepts - Loaded 100000 halfmaps.
[info] 15:32:45.664 [main] INFO  org.renci.umls.db.DbConcepts - Loaded 200000 halfmaps.
[info] 15:32:46.638 [main] INFO  org.renci.umls.db.DbConcepts - Loaded 300000 halfmaps.
[info] 15:32:47.398 [main] INFO  org.renci.umls.db.DbConcepts - Loaded 400000 halfmaps.
[info] 15:32:48.055 [main] INFO  org.renci.umls.db.DbConcepts - Loaded 500000 halfmaps.
[info] 15:32:49.106 [main] INFO  org.renci.umls.db.DbConcepts - Loaded 600000 halfmaps.
[info] 15:32:49.803 [main] INFO  org.renci.umls.db.DbConcepts - Loaded 700000 halfmaps.
[info] 15:32:51.134 [main] INFO  org.renci.umls.db.DbConcepts - Loaded 800000 halfmaps.
[info] 15:32:51.602 [main] INFO  org.renci.umls.db.DbConcepts - Loaded 900000 halfmaps.
[info] 15:32:51.742 [main] INFO  org.renci.umls.db.DbConcepts - 914827 halfmaps loaded.
[info] 15:32:51.748 [main] INFO  org.renci.umls.db.DbConcepts - Loading halfmaps for NCI
[info] 15:32:53.637 [main] INFO  org.renci.umls.db.DbConcepts - Loaded 100000 halfmaps.
[info] 15:32:54.506 [main] INFO  org.renci.umls.db.DbConcepts - Loaded 200000 halfmaps.
[info] 15:32:55.204 [main] INFO  org.renci.umls.db.DbConcepts - Loaded 300000 halfmaps.
[info] 15:32:55.473 [main] INFO  org.renci.umls.db.DbConcepts - 343103 halfmaps loaded.
[success] Total time: 26 s, completed Apr 15, 2020, 3:32:58 PM
```
