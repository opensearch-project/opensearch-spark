package org.opensearch.sql;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.CatalogExtension;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

/**
 * Delegate to V2SessionCatalog in current SparkSession.
 *
 * spark_catalog default catalog name. With FlintDelegatingSessionCatalog, User could customize
 * catalog name, the implementation delegate to V2SessionCatalog in current SparkSession.
 *
 * Usage
 *   --conf spark.sql.catalog.mycatalog = org.opensearch.sql.FlintDelegatingSessionCatalog
 */
public class FlintDelegatingSessionCatalog implements CatalogExtension {
  private CatalogPlugin delegate;

  public final void setDelegateCatalog(CatalogPlugin delegate) {
    // do nothing
  }

  /**
   * Using V2SessionCatalog name: spark_catalog.
   */
  @Override
  public String name() {
    return delegate.name();
  }

  @Override
  public final void initialize(String name, CaseInsensitiveStringMap options) {
    this.delegate =
        SparkSession.getActiveSession().get().sessionState().catalogManager().v2SessionCatalog();
  }

  @Override
  public String[] defaultNamespace() {
    return delegate.defaultNamespace();
  }

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    return asTableCatalog().listTables(namespace);
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    return asTableCatalog().loadTable(ident);
  }

  @Override
  public Table loadTable(Identifier ident, long timestamp) throws NoSuchTableException {
    return asTableCatalog().loadTable(ident, timestamp);
  }

  @Override
  public Table loadTable(Identifier ident, String version) throws NoSuchTableException {
    return asTableCatalog().loadTable(ident, version);
  }

  @Override
  public void invalidateTable(Identifier ident) {
    asTableCatalog().invalidateTable(ident);
  }

  @Override
  public boolean tableExists(Identifier ident) {
    return asTableCatalog().tableExists(ident);
  }

  @Override
  public Table createTable(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties) throws TableAlreadyExistsException, NoSuchNamespaceException {
    return asTableCatalog().createTable(ident, schema, partitions, properties);
  }

  @Override
  public Table alterTable(
      Identifier ident,
      TableChange... changes) throws NoSuchTableException {
    return asTableCatalog().alterTable(ident, changes);
  }

  @Override
  public boolean dropTable(Identifier ident) {
    return asTableCatalog().dropTable(ident);
  }

  @Override
  public boolean purgeTable(Identifier ident) {
    return asTableCatalog().purgeTable(ident);
  }

  @Override
  public void renameTable(
      Identifier oldIdent,
      Identifier newIdent) throws NoSuchTableException, TableAlreadyExistsException {
    asTableCatalog().renameTable(oldIdent, newIdent);
  }

  @Override
  public String[][] listNamespaces() throws NoSuchNamespaceException {
    return asNamespaceCatalog().listNamespaces();
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    return asNamespaceCatalog().listNamespaces(namespace);
  }

  @Override
  public boolean namespaceExists(String[] namespace) {
    return asNamespaceCatalog().namespaceExists(namespace);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(
      String[] namespace) throws NoSuchNamespaceException {
    return asNamespaceCatalog().loadNamespaceMetadata(namespace);
  }

  @Override
  public void createNamespace(
      String[] namespace,
      Map<String, String> metadata) throws NamespaceAlreadyExistsException {
    asNamespaceCatalog().createNamespace(namespace, metadata);
  }

  @Override
  public void alterNamespace(
      String[] namespace,
      NamespaceChange... changes) throws NoSuchNamespaceException {
    asNamespaceCatalog().alterNamespace(namespace, changes);
  }

  @Override
  public boolean dropNamespace(
      String[] namespace,
      boolean cascade) throws NoSuchNamespaceException, NonEmptyNamespaceException {
    return asNamespaceCatalog().dropNamespace(namespace, cascade);
  }

  @Override
  public UnboundFunction loadFunction(Identifier ident) throws NoSuchFunctionException {
    return asFunctionCatalog().loadFunction(ident);
  }

  @Override
  public Identifier[] listFunctions(String[] namespace) throws NoSuchNamespaceException {
    return asFunctionCatalog().listFunctions(namespace);
  }

  @Override
  public boolean functionExists(Identifier ident) {
    return asFunctionCatalog().functionExists(ident);
  }

  private TableCatalog asTableCatalog() {
    return (TableCatalog) delegate;
  }

  private SupportsNamespaces asNamespaceCatalog() {
    return (SupportsNamespaces) delegate;
  }

  private FunctionCatalog asFunctionCatalog() {
    return (FunctionCatalog) delegate;
  }
}
