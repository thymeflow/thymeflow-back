package com.thymeflow.rdf.repository

import java.nio.file.Paths

import com.thymeflow.rdf.Converters._
import com.thymeflow.rdf.repository.RepositoryFactory.{DiskStoreConfig, MemoryStoreConfig, SailConfig}
import org.openrdf.IsolationLevels

/**
  * @author David Montoya
  */
object RepositoryMigration extends App {
  if (args.length == 2) {
    val fromPath = Paths.get(args(0))
    val toPath = Paths.get(args(1))
    val batchCommitSize = 10000

    val fromConfig = RepositoryFactory.SailRepositoryConfig(
      sailConfig = SailConfig(MemoryStoreConfig(persistToDisk = true)),
      dataDirectory = fromPath,
      isolationLevel = IsolationLevels.NONE
    )
    val toConfig = RepositoryFactory.SailRepositoryConfig(
      sailConfig = SailConfig(DiskStoreConfig()),
      dataDirectory = toPath,
      isolationLevel = IsolationLevels.NONE
    )

    val from = RepositoryFactory.initializeRepository(fromConfig, None)
    val to = RepositoryFactory.initializeRepository(toConfig, None)

    val fromConnection = from.newConnection()
    val toConnection = to.newConnection()

    for (namespace <- fromConnection.getNamespaces) {
      toConnection.setNamespace(namespace.getPrefix, namespace.getName)
    }
    for (statements <- fromConnection.getStatements(null, null, null).grouped(batchCommitSize)) {
      toConnection.begin()
      for (statement <- statements) {
        toConnection.add(statement.getSubject, statement.getPredicate, statement.getObject, statement.getContext)
      }
      toConnection.commit()
    }

  } else {
    System.out.print("Expected two arguments: from and to data directories.")
  }
}
