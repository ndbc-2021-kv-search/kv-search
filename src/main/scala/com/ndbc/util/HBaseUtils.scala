package com.ndbc.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HBaseAdmin}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf

object HBaseUtils {
  def jobConfig(tableName: String): JobConf = {
    val conf = hadoopConfig()
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf
  }

  /**
   * true if table exists already
   */
  def isTableExist(tableName: String): Boolean = {
    val conn = getConnection
    val admin = conn.getAdmin.asInstanceOf[HBaseAdmin]
    admin.tableExists(tableName)
  }

  /**
   * create table if not exists
   */
  def createTableIfNotExists(tableName: String, columnFamilies: Seq[String]): Unit = {
    val conn = getConnection
    val admin = conn.getAdmin.asInstanceOf[HBaseAdmin]

    if (!admin.tableExists(TableName.valueOf(tableName))) {
      try {
        val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
        columnFamilies.foreach(columnFamilyName => tableDesc.addFamily(new HColumnDescriptor(columnFamilyName)))
        admin.createTable(tableDesc)
      }
      catch {
        case _: Exception => throw new RuntimeException(s"create hbase table failed : $tableName")
      }
    }
    conn.close()
  }

  /**
   * get connection
   */
  def getConnection: Connection = ConnectionFactory.createConnection(hadoopConfig())

  private def hadoopConfig(): Configuration = {
    val hadoopConf = HBaseConfiguration.create()
    hadoopConf.set("hbase.zookeeper.quorum", "xxx")
    hadoopConf.set("hbase.zookeeper.property.clientPort", "xxx")

    hadoopConf
  }
}
