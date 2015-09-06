package com.github.lucene.store.database;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map.Entry;

import javax.annotation.Resource;
import javax.naming.directory.SearchResult;
import javax.sql.DataSource;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;

/**
 * Handles lucene searches.
 *
 * @author Archimedes
 */
@Stateless
@Remote(DoxSearch.class)
public class LuceneDoxSearchBean implements DoxSearch {

    private static final String FIELD_COLLECTION = "\t collection";

    private static final String FIELD_ID = "\t id";

    private static final String FIELD_INDEX = "\t index";

    private static final String FIELD_TEXT = "\t text";

    private static final String FIELD_UNIQUE_ID = "\t uid";

    /**
     * Table name for the search index.
     */
    private static final String SEARCHINDEX = "SEARCHINDEX";

    /**
     * The data source. It is required that the datasource be XA enabled so it can co-exist with JPA and other
     * operations.
     */
    @Resource
    private DataSource ds;

    @EJB
    private SingletonLockFactory lockFactory;

    @Override
    @Asynchronous
    public void addToIndex(final IndexView... indexViews) {

        try (final Connection c = ds.getConnection()) {
            final JdbcDirectory dir = new JdbcDirectory(c, lockFactory, SEARCHINDEX);
            final Analyzer analyzer = new StandardAnalyzer();
            final IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
            iwc.setWriteLockTimeout(5000);

            try (final IndexWriter indexWriter = new IndexWriter(dir, iwc)) {
                for (final IndexView indexView : indexViews) {
                    final Document doc = buildFromIndexView(indexView);
                    indexWriter.updateDocument(new Term(FIELD_UNIQUE_ID, uid(indexView)), doc);
                }
            }
        } catch (final IOException | SQLException e) {
            throw new PersistenceException(e);
        }
    }

    private IndexView buildFromDoc(final Document doc) {

        final IndexView ret = new IndexView();
        for (final IndexableField field : doc.getFields()) {
            if (FIELD_ID.equals(field.name()) || FIELD_UNIQUE_ID.equals(field.name())
                    || FIELD_COLLECTION.equals(field.name())) {
                continue;
            }
            final Number numericValue = field.numericValue();
            if (numericValue == null) {
                ret.setString(field.name(), field.stringValue());
            } else if (numericValue instanceof Double) {
                ret.setDouble(field.name(), numericValue.doubleValue());
            } else if (numericValue instanceof Long) {
                ret.setLong(field.name(), numericValue.longValue());
            }
        }
        final String idValue = doc.get(FIELD_ID);
        if (idValue != null) {
            ret.setDoxID(new DoxID(idValue));
        } else {
            ret.setMasked(true);
        }
        ret.setCollection(doc.get(FIELD_COLLECTION));
        return ret;

    }

    private Document buildFromIndexView(final IndexView indexView) {

        final Document doc = new Document();
        doc.add(new StringField(FIELD_UNIQUE_ID, uid(indexView), Store.NO));
        doc.add(new StringField(FIELD_INDEX, indexView.getIndex(), Store.NO));
        doc.add(new StringField(FIELD_COLLECTION, indexView.getCollection(), Store.YES));
        if (!indexView.isMasked()) {
            doc.add(new StringField(FIELD_ID, indexView.getDoxID().toString(), Store.YES));
        } else {
            doc.add(new StringField(FIELD_ID, indexView.getDoxID().toString(), Store.NO));
        }
        for (final Entry<String, String> entry : indexView.getStrings()) {
            doc.add(new StringField(entry.getKey(), entry.getValue(), Store.YES));
        }
        for (final Entry<String, String> entry : indexView.getTexts()) {
            doc.add(new TextField(entry.getKey(), entry.getValue(), Store.NO));
        }
        for (final Entry<String, Double> entry : indexView.getDoubles()) {
            doc.add(new DoubleField(entry.getKey(), entry.getValue(), Store.YES));
        }
        for (final Entry<String, Long> entry : indexView.getLongs()) {
            doc.add(new LongField(entry.getKey(), entry.getValue(), Store.YES));
        }
        doc.add(new TextField(FIELD_TEXT, indexView.getText(), Store.NO));
        return doc;

    }

    private SearchResult buildSearchResults(final IndexSearcher indexSearcher, final TopDocs search) throws IOException {

        final SearchResult result = new SearchResult();
        result.setTotalHits(search.totalHits);
        for (final ScoreDoc scoreDoc : search.scoreDocs) {
            final Document doc = indexSearcher.doc(scoreDoc.doc);

            result.addHit(buildFromDoc(doc));
        }
        return result;
    }

    @Override
    @Asynchronous
    public void removeFromIndex(final String collection, final DoxID doxID) {

        try (final Connection c = ds.getConnection()) {
            final Analyzer analyzer = new StandardAnalyzer();
            final IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
            final JdbcDirectory dir = new JdbcDirectory(c, lockFactory, SEARCHINDEX);
            try (final IndexWriter indexWriter = new IndexWriter(dir, iwc)) {
                final BooleanQuery booleanQuery = new BooleanQuery();
                booleanQuery.add(new TermQuery(new Term(FIELD_ID, doxID.toString())), Occur.MUST);
                booleanQuery.add(new TermQuery(new Term(FIELD_COLLECTION, collection)), Occur.MUST);
                indexWriter.deleteDocuments(booleanQuery);
            }
        } catch (final IOException | SQLException e) {
            throw new PersistenceException(e);
        }
    }

    /**
     * This will clear all the indexing data from the system.
     */
    @Override
    public void reset() {

        try (final Connection c = ds.getConnection()) {
            final Analyzer analyzer = new StandardAnalyzer();
            final IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
            final JdbcDirectory dir = new JdbcDirectory(c, lockFactory, SEARCHINDEX);
            try (final IndexWriter indexWriter = new IndexWriter(dir, iwc)) {
                indexWriter.deleteAll();
            }
        } catch (final IOException | SQLException e) {
            throw new PersistenceException(e);
        }

    }

    @Override
    public SearchResult search(final String index, final String queryString, final int limit) {

        // TODO verify access to index for user

        try (final Connection c = ds.getConnection()) {
            final Analyzer analyzer = new StandardAnalyzer();
            final IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

            final QueryParser parser = new QueryParser(FIELD_TEXT, analyzer);
            final Query query = parser.parse(queryString);
            final BooleanQuery booleanQuery = new BooleanQuery();
            booleanQuery.add(query, Occur.MUST);
            booleanQuery.add(new TermQuery(new Term(FIELD_INDEX, index)), Occur.MUST);
            final JdbcDirectory dir = new JdbcDirectory(c, lockFactory, SEARCHINDEX);
            try (final IndexWriter indexWriter = new IndexWriter(dir, iwc)) {

                final IndexSearcher indexSearcher = new IndexSearcher(DirectoryReader.open(indexWriter, true));
                final TopDocs search = indexSearcher.search(booleanQuery, limit);

                return buildSearchResults(indexSearcher, search);
            }
        } catch (final IOException | ParseException | SQLException e) {
            throw new PersistenceException(e);
        }
    }

    private String uid(final IndexView view) {

        return view.getIndex() + "/" + view.getCollection() + "/" + view.getDoxID();
    }

}
