/*
 * Copyright 2017 47 Degrees, LLC. <http://www.47deg.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package freestyle.cassandra
package util

import cats.instances.list._
import cats.instances.try_._
import cats.syntax.traverse._
import com.datastax.driver.core.{Cluster, Session}
//import org.apache.cassandra.service.CassandraDaemon

import scala.concurrent.Future
import scala.reflect.io.Path
import scala.util.Try
import scala.concurrent.ExecutionContext.Implicits.global

object CassandraUtil {

  import CassandraConfigurationValues._

//  def startCassandra(): Try[Future[CassandraDaemon]] =
//    for {
//      _ <- deleteDir(baseDir.getAbsolutePath)
//      _ <- setSystemProperties()
//      futureDaemon <- Try {
//        Future {
//          val daemon = new CassandraDaemon()
//          daemon.activate()
//          daemon.start()
//          daemon
//        }
//      }
//    } yield futureDaemon

  def executeCQL(schemaPath: String): Try[Unit] = {

    def buildCluster(): Try[Cluster] = Try {
      new Cluster.Builder()
        .withClusterName(clusterName)
        .addContactPoint(listenAddress)
        .withPort(nativePort)
        .build()
    }

    def connectCluster(cluster: Cluster): Try[Session] = Try(cluster.connect())

    def readStatements(): Try[List[String]] =
      Try(scala.io.Source.fromInputStream(getClass.getResourceAsStream(schemaPath)).mkString).map {
        content => content.split(";").filter(_.trim.nonEmpty).toList
      }

    def executeStatement(session: Session, statement: String): Try[Unit] =
      Try(session.execute(statement)).map(_ => (): Unit)

    buildCluster() flatMap { cluster =>
      val tryExecuteStatements: Try[Unit] = for {
        session    <- connectCluster(cluster)
        statements <- readStatements()
        _          <- statements.traverse(executeStatement(session, _)).map(_ => (): Unit)
        _          <- Try(session.close())
      } yield ()
      Try(cluster.close())
      tryExecuteStatements
    }
  }

//  def stopCassandra(daemon: CassandraDaemon): Try[Future[Unit]] =
//    Try(Future(daemon.deactivate())).map { future =>
//      future.map(_ => deleteDir(baseDir.getAbsolutePath, recreate = false))
//    }
//
//  def setLogLevelToWarn(): Try[Unit] = Try {
//    import org.slf4j.LoggerFactory
//    import ch.qos.logback.classic.Level
//    import ch.qos.logback.classic.Logger
//
//    val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger]
//    root.setLevel(Level.WARN)
//  }
//
//  private[this] def deleteDir(pathDir: String, recreate: Boolean = true): Try[Unit] = {
//    val path: Path = Path(pathDir)
//    Try(path.deleteRecursively()).map { _ =>
//      if (recreate) path.createDirectory(force = true)
//      (): Unit
//    }
//  }
//
//  private[this] def setSystemProperties(): Try[Unit] = Try {
//    System.setProperty(
//      "cassandra.config.loader",
//      classOf[CassandraConfigurationLoader].getCanonicalName)
//    System.setProperty("cassandra-foreground", "true")
//    System.setProperty("cassandra.native.epoll.enabled", "false")
//    System.setProperty("cassandra.storagedir", storageDir.getAbsolutePath)
//  }

}
