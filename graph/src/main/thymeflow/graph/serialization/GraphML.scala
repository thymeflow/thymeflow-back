package thymeflow.graph.serialization

import java.io.{BufferedWriter, OutputStreamWriter}
import java.nio.file.{Files, Path}

import scala.xml.{Elem, Node}

/**
  * @author David Montoya
  */
object GraphML {

  def edge(id: String, source: String, target: String, attributes: Traversable[(String, String)] = Traversable.empty) = {
    val attributesXML = elementAttributes(attributes)
    <edge id={id.toString} source={source} target={target}>
      {attributesXML}
    </edge>
  }

  def elementAttributes(attributes: Traversable[(String, String)]) = {
    attributes.map {
      case (key, content) => <data key={key}>
        {content}
      </data>
    }
  }

  def node(id: String, attributes: Traversable[(String, String)] = Traversable.empty) = {
    val attributesXML = elementAttributes(attributes)
    <node id={id}>
      {attributesXML}
    </node>
  }

  def nodeKeys(keys: Traversable[(String, String, String)]) = {
    graphKeys(keys.map(("node", _)))
  }

  def edgeKeys(keys: Traversable[(String, String, String)]) = {
    graphKeys(keys.map(("edge", _)))
  }

  def graphKeys(keys: Traversable[(String, (String, String, String))]) = {
    keys.map {
      case (attributeFor, (id, attributeName, attributeType)) => <key id={id} for={attributeFor} attr.name={attributeName} attr.type={attributeType}></key>
    }
  }

  def keys(keys: Traversable[(String, String, String, String)]) = {
    keys.map {
      case (attributeId, attributeFor, attributeName, attributeType) => <key id={attributeId} for={attributeFor} attr.name={attributeName} attr.type={attributeType}></key>
    }
  }

  def graph(graphId: String,
            directed: Boolean,
            keys: Traversable[Node],
            nodes: Traversable[Node],
            edges: Traversable[Node]): Elem = {
    <graphml xmlns="http://graphml.graphdrawing.org/xmlns"
             xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
             xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">
      {keys}<graph id={graphId} edgedefault={if (directed) "directed" else "undirected"}>
      {nodes}{edges}
    </graph>
    </graphml>
  }

  def write(path: Path, graphML: Elem) = {
    val writer = new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(path), "UTF-8"))
    // WRITE BOM (needed for Gephi)
    writer.append('\uFEFF')
    scala.xml.XML.write(writer, graphML, "UTF-8", xmlDecl = true, null)
    writer.close()
  }
}
