package org.renci.sssom.ontologies

import java.io.File
import java.net.{HttpURLConnection, URI, URL}

import org.apache.commons.dbcp2.DriverManagerConnectionFactory
import org.apache.jena.graph.impl.LiteralLabel
import org.apache.jena.graph.{Graph, Node, NodeFactory, Triple}
import org.apache.jena.ontology.OntModelSpec
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.system.StreamRDF
import org.apache.jena.riot.{Lang, RDFParser}
import org.apache.jena.sparql.core.Quad
import org.apache.jena.vocabulary.RDFS
import org.renci.sssom.SSSOMFiller
import org.renci.sssom.SSSOMFiller.Row

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * A generic class for filling in SSSOM files with terms from an ontology.
  */
case class OntologyFiller(ontology: File) extends SSSOMFiller {
  /**
    * When the class is initialized, it should open the ontology to look into it.
    * However, we need to store it in an SQLite database for quicker access.
    */
  /*
  lazy val sqliteDb: DriverManagerConnectionFactory = new DriverManagerConnectionFactory(
    "jdbc:sqlite:./ontologies.sqlite"
  )

  val conn = sqliteDb.createConnection()
  val results = Try {
    val checkCount = conn.prepareStatement("SELECT COUNT(*) AS cnt FROM ontologies WHERE url=?")
    checkCount.setString(1, url.toString)
    checkCount.executeQuery()
  }
  val rowsFromDb = results.map(_.getInt(1)).getOrElse(-1)
  conn.close()

  if (rowsFromDb <= 0) {
    scribe.info(s"No labels from ontology $ontology in ontologies SQLite table, regenerating.")

    val exampleURL = "http://example.org/"
    val exampleGraph = NodeFactory.createURI(exampleURL.toString)

    RDFParser.source(ontology.getAbsolutePath).lang(Lang.RDFXML).parse(new StreamRDF {
      override def start(): Unit = {}

      override def triple(triple: Triple): Unit = quad(new Quad(exampleGraph, triple))

      override def quad(quad: Quad): Unit = {
        if (quad.getPredicate.getURI.equals(RDFS.label.getURI)) {
          val subject: URI = new URI(quad.getSubject.getURI)
          val label: LiteralLabel = quad.getObject.getLiteral
          val labelLanguage: String = label.language()
          scribe.debug(s"Found label: ${subject} has label ${label.getValue}@$labelLanguage.")
        }
      }

      override def base(base: String): Unit = {}

      override def prefix(prefix: String, iri: String): Unit = {}

      override def finish(): Unit = {}
    })
    scribe.info("Completed RDFParser")
  }*/

  val owlModelSpec = new OntModelSpec( OntModelSpec.OWL_LITE_MEM )
  val owlModel = ModelFactory.createOntologyModel(owlModelSpec)
  owlModel.read(ontology.getAbsolutePath)
  val ontologyGraph: Graph = owlModel.getGraph
  scribe.info(s"Loaded ontology $ontology containing ${owlModel.getGraph.size()} triples.")

  /**
    * Fill in the input row. The list of all headers is also provided.
    *
    * @return None if this row could not be filled, and Some[Row] if it can.
    */
  override def fillRow(row: Row, headers: List[String]): Option[Seq[SSSOMFiller.Result]] = {
    val subjectId = row.getOrElse("subject_id", "")
    val subjectLabels = row.getOrElse("subject_label", "").split("\\s*\\|\\s*").toSeq

    val predicates: Set[Node] = Set(
      RDFS.label.asNode(),
      NodeFactory.createURI("http://www.geneontology.org/formats/oboInOwl#hasRelatedSynonym")
    )

    val results = subjectLabels flatMap { subjectLabel =>
      ontologyGraph.find(null, null, NodeFactory.createLiteral(subjectLabel))
        .filterKeep(triple => predicates.contains(triple.getPredicate))
        .mapWith(triple => {
          scribe.info(s"Found match! Subject label $subjectLabel mapped to triple $triple")

          val subjectURI = triple.getSubject.getURI
          val subjectLabels = ontologyGraph.find(triple.getSubject, RDFS.label.asNode(), null).asScala.map(_.getObject.toString(false))

          val result = SSSOMFiller.Result(
            row,
            row + ("predicate_id" -> subjectURI)
              + ("predicate_label" -> subjectLabels.mkString("|"))
              + ("comment" -> s"Ontology $ontology contains triple asserting $triple"),
            this
          )
          scribe.info(s"Result: $result")
          result
        }).asScala
    }

    if (results.isEmpty) None else Some(results)
  }
}

object OntologyFiller {
  def getRedirectedURL(sourceURL: URL): URL = {
    // Find out where this URL redirects to.
    val conn = sourceURL.openConnection().asInstanceOf[HttpURLConnection]
    conn.setInstanceFollowRedirects(false)
    if (conn.getResponseCode == HttpURLConnection.HTTP_MOVED_TEMP || conn.getResponseCode == HttpURLConnection.HTTP_MOVED_PERM) {
      val location = conn.getHeaderField("Location")
      if (location == null) throw new RuntimeException(s"Redirect without 'Location' provided: ${conn}")
      else getRedirectedURL(new URL(location))
    } else sourceURL
  }
}
