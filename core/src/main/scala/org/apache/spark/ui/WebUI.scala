/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ui

import java.util.EnumSet
import javax.servlet.DispatcherType
import javax.servlet.http.{HttpServlet, HttpServletRequest}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.xml.Node

import org.eclipse.jetty.servlet.{FilterHolder, FilterMapping, ServletContextHandler, ServletHolder}
import org.json4s.JsonAST.{JNothing, JValue}

import org.apache.spark.{SecurityManager, SparkConf, SSLOptions}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.util.Utils

/**
 * The top level component of the UI hierarchy that contains the server.
 *
 * Each WebUI represents a collection of tabs, each of which in turn represents a collection of
 * pages. The use of tabs is optional, however; a WebUI may choose to include pages directly.
 */
private[spark] abstract class WebUI(
    val securityManager: SecurityManager,
    val sslOptions: SSLOptions,
    port: Int,
    conf: SparkConf,
    basePath: String = "",
    name: String = "")
  extends Logging {

  // WebUITab的缓存数组
  protected val tabs = ArrayBuffer[WebUITab]()
//  ServletContextHandler的缓冲数组
  protected val handlers = ArrayBuffer[ServletContextHandler]()
//  WebUIPage与ServletContextHandler缓冲数组之间的映射关系
  protected val pageToHandlers = new HashMap[WebUIPage, ArrayBuffer[ServletContextHandler]]
//  用于缓存ServerInfo,即WebUI的Jetty服务信息
  protected var serverInfo: Option[ServerInfo] = None
  // 当前WebUI的jetty服务的主机名。优先采用系统环境变量SPARK_PUBLIC_DNS指定的主机名，否则采用spark.driver.host属性指定的host
  protected val publicHostName = Option(conf.getenv("SPARK_PUBLIC_DNS")).getOrElse(
    conf.get(DRIVER_HOST_ADDRESS))
//  过滤了$符号的当前类的简单名称
  private val className = Utils.getFormattedClassName(this)

  def getBasePath: String = basePath
//  获取tab中所有的WebUITab
  def getTabs: Seq[WebUITab] = tabs
//  获取handler中所有的ServletContextHandler
  def getHandlers: Seq[ServletContextHandler] = handlers
  def getSecurityManager: SecurityManager = securityManager

  def getDelegatingHandlers: Seq[DelegatingServletContextHandler] = {
    handlers.map(new DelegatingServletContextHandler(_))
  }

  // 向tabs中添加WebUITab
  /** Attaches a tab to this UI, along with all of its attached pages. */
  def attachTab(tab: WebUITab): Unit = {
    tab.pages.foreach(attachPage)
    tabs += tab
  }

  /** Detaches a tab from this UI, along with all of its attached pages. */
  def detachTab(tab: WebUITab): Unit = {
    tab.pages.foreach(detachPage)
    tabs -= tab
  }

  /** Detaches a page from this UI, along with all of its attached handlers. */
  def detachPage(page: WebUIPage): Unit = {
    pageToHandlers.remove(page).foreach(_.foreach(detachHandler))
  }

  /** Attaches a page to this UI. */
  def attachPage(page: WebUIPage): Unit = {
    val pagePath = "/" + page.prefix
//    给WebUIPage创建与render与renderJson两个方法分别关联的ServletContextHandler
    val renderHandler = createServletHandler(pagePath,
      (request: HttpServletRequest) => page.render(request), securityManager, conf, basePath)
    val renderJsonHandler = createServletHandler(pagePath.stripSuffix("/") + "/json",
      (request: HttpServletRequest) => page.renderJson(request), securityManager, conf, basePath)
    attachHandler(renderHandler)
    attachHandler(renderJsonHandler)
    val handlers = pageToHandlers.getOrElseUpdate(page, ArrayBuffer[ServletContextHandler]())
    handlers += renderHandler
    handlers += renderJsonHandler
  }

  /**
   * 给handlers缓存数组中添加ServletContextHandler，并且将此ServletContextHandler通过ServerInfo的addHandler方法
   * 添加到Jetty服务器中。
   */
  /** Attaches a handler to this UI. */
  def attachHandler(handler: ServletContextHandler): Unit = {
    handlers += handler
    serverInfo.foreach(_.addHandler(handler))
  }

  /** Attaches a handler to this UI. */
  def attachHandler(contextPath: String, httpServlet: HttpServlet, pathSpec: String): Unit = {
    val ctx = new ServletContextHandler()
    ctx.setContextPath(contextPath)
    ctx.addServlet(new ServletHolder(httpServlet), pathSpec)
    attachHandler(ctx)
  }

  /**
   * 从handlers缓存数组中移除ServletContextHandler，并且将此ServletContextHandler通过ServerInfo的removeHandler方法从
   * Jetty服务中移除。
   */
  /** Detaches a handler from this UI. */
  def detachHandler(handler: ServletContextHandler): Unit = {
    handlers -= handler
    serverInfo.foreach(_.removeHandler(handler))
  }

  /**
   * Detaches the content handler at `path` URI.
   *
   * @param path Path in UI to unmount.
   */
  def detachHandler(path: String): Unit = {
    handlers.find(_.getContextPath() == path).foreach(detachHandler)
  }

  /**
   * Adds a handler for static content.
   *
   * @param resourceBase Root of where to find resources to serve.
   * @param path Path in UI where to mount the resources.
   */
  def addStaticHandler(resourceBase: String, path: String = "/static"): Unit = {
    attachHandler(JettyUtils.createStaticHandler(resourceBase, path))
  }

  /** A hook to initialize components of the UI */
  // 用于初始化WebUI服务中的所有组件。
  def initialize(): Unit

  /** Binds to the HTTP server behind this web interface. */
  def bind(): Unit = {
    assert(serverInfo.isEmpty, s"Attempted to bind $className more than once!")
    try {
      val host = Option(conf.getenv("SPARK_LOCAL_IP")).getOrElse("0.0.0.0")
      serverInfo = Some(startJettyServer(host, port, sslOptions, handlers, conf, name))
      logInfo(s"Bound $className to $host, and started at $webUrl")
    } catch {
      case e: Exception =>
        logError(s"Failed to bind $className", e)
        System.exit(1)
    }
  }

  /** @return The url of web interface. Only valid after [[bind]]. */
  def webUrl: String = s"http://$publicHostName:$boundPort"

  /** @return The actual port to which this server is bound. Only valid after [[bind]]. */
  def boundPort: Int = serverInfo.map(_.boundPort).getOrElse(-1)

  /** Stops the server behind this web interface. Only valid after [[bind]]. */
  def stop(): Unit = {
    assert(serverInfo.isDefined,
      s"Attempted to stop $className before binding to a server!")
    serverInfo.foreach(_.stop())
  }
}


/**
 * 标签页
 * prefix: 当前WebUITab的前缀，prefix将与上级节点的路径一起构成当前WebUITab的访问路径
 *
 * A tab that represents a collection of pages.
 * The prefix is appended to the parent address to form a full path, and must not contain slashes.
 */
private[spark] abstract class WebUITab(parent: WebUI, val prefix: String) {
  // 当前WebUITab所包含的WebUIPage的缓冲数组
  val pages = ArrayBuffer[WebUIPage]()
  // 当前WebUITab的名称。name实际是将prefix的首字母转换成大写字母后取得
  val name = prefix.capitalize

//  首先将当前WebUITab的前缀与WebUIPage的前缀拼接，作为WebUIPage的访问路径，然后向pages中添加WebUIPage
  /** Attach a page to this tab. This prepends the page's prefix with the tab's own prefix. */
  def attachPage(page: WebUIPage) {
    page.prefix = (prefix + "/" + page.prefix).stripSuffix("/")
    pages += page
  }

  /**
   *  获取父亲WebUI中的所有WebUITab
   */
  /** Get a list of header tabs from the parent UI. */
  def headerTabs: Seq[WebUITab] = parent.getTabs

  /**
   * 获取父亲WebUI的基本路径
   * @return
   */
  def basePath: String = parent.getBasePath
}


/**
 * A page that represents the leaf node in the UI hierarchy.
 *
 * The direct parent of a WebUIPage is not specified as it can be either a WebUI or a WebUITab.
 * If the parent is a WebUI, the prefix is appended to the parent's address to form a full path.
 * Else, if the parent is a WebUITab, the prefix is appended to the super prefix of the parent
 * to form a relative path. The prefix must not contain slashes.
 */
private[spark] abstract class WebUIPage(var prefix: String) {
  // 渲染页面
  def render(request: HttpServletRequest): Seq[Node]
  // 生成JSON
  def renderJson(request: HttpServletRequest): JValue = JNothing
}

private[spark] class DelegatingServletContextHandler(handler: ServletContextHandler) {

  def prependFilterMapping(
      filterName: String,
      spec: String,
      types: EnumSet[DispatcherType]): Unit = {
    val mapping = new FilterMapping()
    mapping.setFilterName(filterName)
    mapping.setPathSpec(spec)
    mapping.setDispatcherTypes(types)
    handler.getServletHandler.prependFilterMapping(mapping)
  }

  def addFilter(
      filterName: String,
      className: String,
      filterParams: Map[String, String]): Unit = {
    val filterHolder = new FilterHolder()
    filterHolder.setName(filterName)
    filterHolder.setClassName(className)
    filterParams.foreach { case (k, v) => filterHolder.setInitParameter(k, v) }
    handler.getServletHandler.addFilter(filterHolder)
  }

  def filterCount(): Int = {
    handler.getServletHandler.getFilters.length
  }

  def getContextPath(): String = {
    handler.getContextPath
  }
}
