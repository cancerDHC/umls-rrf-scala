# UMLS/NCImt/NCIt tools
This is a set of tools for working with the UMLS RRF format,
NCI Metathesaurus and NCI Thesaurus.

## umls-rrf-scala
A very basic library for parsing files in the UMLS RRF format.

This program expects the `META` directory from the
[NLM UMLS download](https://www.nlm.nih.gov/research/umls/index.html)
to be in the directory where the program is executed. The `--rrf-dir`
command line argument can also be used to point to the `META` directory
in another directory. Output is produced in the
[SSSOM format](https://github.com/OBOFoundry/SSSOM).

```console
$ sbt "run --from-source SNOMEDCT_US --to-source NCI -o mappings.tsv"
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

### Filler
Filler takes in a set of mappings in the SSSOM format and attempts to fill in
missing mappings based on its command line arguments.

```console
$ JAVA_OPTS="-Xmx16G" sbt "runMain org.renci.sssom.Filler --input-file matched-with-ols-lbo.tsv --output-file matched-with-ols-lbo-hp-chebi.tsv --fill-predicate-id 'skos:narrowMatch' --from-ontology ontologies/hp.owl --from-ontology ontologies/chebi.owl"
[info] Loading settings for project global-plugins from metals.sbt ...
[info] Loading global plugins from /Users/gaurav/.sbt/1.0/plugins
[info] Loading settings for project umls-rrf-scala-build from plugins.sbt ...
[info] Loading project definition from /Users/gaurav/Development/umls-rrf-scala/project
[info] Loading settings for project umls-rrf-scala from build.sbt ...
[info] Set current project to umls-rrf (in build file:/Users/gaurav/Development/umls-rrf-scala/)
[warn] Multiple main classes detected.  Run 'show discoveredMainClasses' to see the list
[info] running (fork) org.renci.sssom.Filler --input-file matched-with-ols-lbo.tsv --output-file matched-with-ols-lbo-hp-chebi.tsv --fill-predicate-id 'skos:narrowMatch' --from-ontology ontologies/hp.owl --from-ontology ontologies/chebi.owl
[error] SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
[error] SLF4J: Defaulting to no-operation (NOP) logger implementation
[error] SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
[info] 2020.09.16 21:14:28 [INFO] org.renci.sssom.ontologies.OntologyFiller:24:14 - Loaded ontology ontologies/hp.owl containing 953183 triples.
[info] 2020.09.16 21:15:03 [INFO] org.renci.sssom.ontologies.OntologyFiller:24:14 - Loaded ontology ontologies/chebi.owl containing 4924892 triples.
[info] 2020.09.16 21:15:03 [INFO] org.renci.sssom.Filler:63:14 - Active row fillers: OntologyFiller(ontologies/hp.owl), OntologyFiller(ontologies/chebi.owl)
[info] 2020.09.16 21:15:04 [INFO] org.renci.sssom.Filler.<local Filler>:95:20 - Filled subject ID SNOMEDCT_US:8836009 with ArraySeq(HashMap(predicate_label -> gallocyanin, predicate_id -> http://purl.obolibrary.org/obo/CHEBI_90106, comment -> Ontology ontologies/chebi.owl contains triple asserting http://purl.obolibrary.org/obo/CHEBI_90106 @http://www.geneontology.org/formats/oboInOwl#hasRelatedSynonym "Gallocyanine", mapping_set_version -> , subject_label -> Gallocyanine stain (substance)|Gallocyanine|Gallocyanine stain, subject_id -> SNOMEDCT_US:8836009, mapping_set_id -> , object_label -> , match_type -> , object_id -> ))
[info] 2020.09.16 21:15:04 [INFO] org.renci.sssom.Filler.<local Filler>:95:20 - Filled subject ID SNOMEDCT_US:11780008 with ArraySeq(HashMap(predicate_label -> Sirius red 4B, predicate_id -> http://purl.obolibrary.org/obo/CHEBI_88191, comment -> Ontology ontologies/chebi.owl contains triple asserting http://purl.obolibrary.org/obo/CHEBI_88191 @http://www.geneontology.org/formats/oboInOwl#hasRelatedSynonym "Chlorantine fast red 5B", mapping_set_version -> , subject_label -> Chlorantine fast red 5B|Durazol red stain|Chlorantine fast red|Durazol red stain (substance)|Durazol red, subject_id -> SNOMEDCT_US:11780008, mapping_set_id -> , object_label -> , match_type -> , object_id -> ))
[info] 2020.09.16 21:15:04 [INFO] org.renci.sssom.Filler.<local Filler>:95:20 - Filled subject ID SNOMEDCT_US:38707008 with ArraySeq(HashMap(predicate_label -> Celestin blue B, predicate_id -> http://purl.obolibrary.org/obo/CHEBI_88183, comment -> Ontology ontologies/chebi.owl contains triple asserting http://purl.obolibrary.org/obo/CHEBI_88183 @http://www.geneontology.org/formats/oboInOwl#hasRelatedSynonym "Mordant blue 14", mapping_set_version -> , subject_label -> Coelestine blue|Coelestine blue B stain|Celestine blue B stain (substance)|Celestine blue|Celestine blue B stain|Mordant blue 14, subject_id -> SNOMEDCT_US:38707008, mapping_set_id -> , object_label -> , match_type -> , object_id -> ))
[...]
[info] 2020.09.16 21:15:04 [INFO] org.renci.sssom.Filler.<local Filler>:95:20 - Filled subject ID SNOMEDCT_US:406969006 with ArraySeq(HashMap(predicate_label -> thionine, predicate_id -> http://purl.obolibrary.org/obo/CHEBI_52295, comment -> Ontology ontologies/chebi.owl contains triple asserting http://purl.obolibrary.org/obo/CHEBI_52295 @http://www.geneontology.org/formats/oboInOwl#hasRelatedSynonym "Thionin", mapping_set_version -> , subject_label -> Thionin|Thionin stain|Thionin stain (substance), subject_id -> SNOMEDCT_US:406969006, mapping_set_id -> , object_label -> , match_type -> , object_id -> ))
[info] 2020.09.16 21:15:04 [INFO] org.renci.sssom.Filler.<local Filler>:95:20 - Filled subject ID SNOMEDCT_US:406990003 with ArraySeq(HashMap(predicate_label -> methyl blue free acid, predicate_id -> http://purl.obolibrary.org/obo/CHEBI_87477, comment -> Ontology ontologies/chebi.owl contains triple asserting http://purl.obolibrary.org/obo/CHEBI_87477 @http://www.geneontology.org/formats/oboInOwl#hasRelatedSynonym "Aniline blue", mapping_set_version -> , subject_label -> Aniline blue stain (substance)|Aniline blue stain|Aniline blue, subject_id -> SNOMEDCT_US:406990003, mapping_set_id -> , object_label -> , match_type -> , object_id -> ))
[info] 2020.09.16 21:15:04 [INFO] org.renci.sssom.Filler:110:14 - Out of 7980 rows:
[info]   - 5546 rows (69.50%) have existing terms.
[info]   - 2426 rows (30.40%) could not be matched.
[info]   - 8 rows (0.10%) could be matched.
[success] Total time: 44 s, completed Sep 16, 2020, 9:15:05 PM
```
