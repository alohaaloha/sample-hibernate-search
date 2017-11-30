
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.core.LowerCaseFilterFactory;
import org.apache.lucene.analysis.standard.StandardTokenizerFactory;
import org.apache.lucene.search.Query;
import org.hibernate.search.FullTextQuery;
import org.hibernate.search.annotations.AnalyzerDef;
import org.hibernate.search.annotations.TokenFilterDef;
import org.hibernate.search.annotations.TokenizerDef;
import org.hibernate.search.batchindexing.impl.SimpleIndexingProgressMonitor;
import org.hibernate.search.engine.ProjectionConstants;
import org.hibernate.search.jpa.FullTextEntityManager;
import org.hibernate.search.query.dsl.MustJunction;
import org.hibernate.search.query.dsl.PhraseMatchingContext;

import javax.ejb.Stateless;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by r.marinkovic on 5/24/2017.
 *
 *  getting started:    http://hibernate.org/search/documentation/getting-started/
 *  making queries:     https://docs.jboss.org/hibernate/search/4.5/reference/en-US/html/search-query.html#search-query-querydsl
 *  mapping:            http://docs.jboss.org/hibernate/stable/search/reference/en-US/html_single/#search-mapping-associated
 *  usage example:
 *   List<Object> res = luceneSearch
 *      .onClass(RouterInterface.class)
 *      .searchTerm(searchTerm)
 *      .from(from)
 *      .to(to)
 *      .typeOfSearch(LuceneSearch.TypeOfSearch.PHRASE_WITH_WILDCARD)
 *      .onField("name")
 *      .onField("router.ipAddress")
 *      .getSearchResults(); // or .getSearchResultsCount();
 *
 */
@Stateless
public class LuceneSearch {

    /**
     * Type of search
     * */
    public enum TypeOfSearch {
        PHRASE, PHRASE_WITH_WILDCARD, ANY_KEYWORD
    }

    // LUCENE REMOVE SOME CHARACHETS WHEN INDEXING
    // "+ - && || ! ( ) { } [ ] ^ \" ~ * ? : \\ /";

    /**
     * JPA entity manager
     * */
    @PersistenceContext
    private EntityManager em;

    /**
     * Part of query builder - executes query and returns result data
     * */
    public List<Object> getSearchResults() {
        //lucine query made using hib query builder
        generateLuceneQuery();
        //create jpa query using lucine query
        if (luceneQuery != null) {
            if (to > 0 && to > from && from >= 0) {
                jpaQuery = fullTextEntityManager.createFullTextQuery(luceneQuery, aClass).setFirstResult(from).setMaxResults(to - from);
            } else {
                jpaQuery = fullTextEntityManager.createFullTextQuery(luceneQuery, aClass);
            }
            //execute jpa query query
            return jpaQuery.getResultList();
        } else {
            return new ArrayList<>();
        }

    }

    /**
     * Part of query builder - executes query and returns result data with metadata
     * */
    public List<Object[]> getSearchResultsWithMetadata() {
        //lucine query made using hib query builder
        generateLuceneQuery();
        //create jpa query using lucine query
        if (luceneQuery != null) {
            if (to > 0 && to > from && from >= 0) {
                jpaQuery = fullTextEntityManager.createFullTextQuery(luceneQuery, aClass)
                        .setProjection(ProjectionConstants.SCORE, ProjectionConstants.THIS, FullTextQuery.SCORE)
                        //.setProjection(FullTextQuery.SCORE, FullTextQuery.THIS)
                        .setFirstResult(from).setMaxResults(to - from);
            } else {
                jpaQuery = fullTextEntityManager.createFullTextQuery(luceneQuery, aClass);
            }
            //execute jpa query query
            return jpaQuery.getResultList();
        } else {
            return new ArrayList<>();
        }

    }

    /**
     * Part of query builder - executes query and returns result size
     *
     * */
    public int getSearchResultsCount() {
        //lucine query made using hib query builder
        generateLuceneQuery();
        //create and execute jpa query using lucine query
        return fullTextEntityManager.createFullTextQuery(luceneQuery, aClass).getResultSize();
    }

    /**
     * Rebuilds search index
     *
     * */
    public static void rebuildIndex(EntityManager em) throws InterruptedException {
        System.out.println("Rebuilding search index ...");
        // Using an EntityManager (JPA) to rebuild an index//
        FullTextEntityManager fullTextEntityManager = org.hibernate.search.jpa.Search.getFullTextEntityManager(em);
        // fullTextEntityManager.createIndexer().startAndWait();

        fullTextEntityManager.createIndexer().purgeAllOnStart(true) // true by default, highly recommended
                .optimizeAfterPurge(true) // true is default, saves some disk space
                .optimizeOnFinish(true) // true by default
                .progressMonitor(new SimpleIndexingProgressMonitor(50000)) // reduce amount of logging
                .startAndWait();
    }

    private Query generateLuceneQuery() {

        fullTextEntityManager = org.hibernate.search.jpa.Search.getFullTextEntityManager(em);
        queryBuilder = fullTextEntityManager.getSearchFactory().buildQueryBuilder().forEntity(aClass).get();

        if (searchTerm != null) {

            // prepare term
            searchTerm = searchTerm.toLowerCase();
            searchTerm = StringUtils.normalizeSpace(searchTerm.replaceAll("[-+&|!(){}^\"~*?:@\\/]+", " "));

            if (!searchTerm.equals(" ") && !searchTerm.equals("")) {

                if (typeOfSearch == TypeOfSearch.PHRASE) {

                    System.out.println("Search for exact phrase: " + searchTerm);
                    //make query
                    PhraseMatchingContext phraseMatchingContext = queryBuilder.phrase().withSlop(10).onField(fields.get(0));
                    if (fields.size() >= 2) {
                        for (int i = 1; i < fields.size(); i++) {
                            phraseMatchingContext.andField(fields.get(i));
                        }
                    }
                    luceneQuery = phraseMatchingContext.sentence(searchTerm).createQuery();

                } else if (typeOfSearch == TypeOfSearch.PHRASE_WITH_WILDCARD) {

                    System.out.println("Search for phrase with wildcard: " + searchTerm);
                    String[] terms = searchTerm.split(" ");
                    MustJunction mustJunction = queryBuilder.bool()
                            .must(queryBuilder.keyword().wildcard().onFields(fields.toArray(new String[] {})).matching(terms[0] + "*").createQuery());
                    for (int i = 1; i < terms.length; i++) {
                        mustJunction.must(queryBuilder.keyword().wildcard().onFields(fields.toArray(new String[] {})).matching(terms[i] + "*").createQuery());
                    }
                    luceneQuery = mustJunction.createQuery();

                } else if (typeOfSearch == TypeOfSearch.ANY_KEYWORD) {

                    System.out.println("Search for any keyword: " + searchTerm);
                    //make query
                    //option: .keyword().fuzzy()
                    luceneQuery = queryBuilder.keyword().onFields(fields.toArray(new String[] {})).matching(searchTerm).createQuery();

                }

            }
        }

        return luceneQuery;
    }

    //full text ent manager
    private FullTextEntityManager fullTextEntityManager;

    //hibernate query builder from full text ent manager
    private org.hibernate.search.query.dsl.QueryBuilder queryBuilder;

    //lucine query made using hib query builder
    private org.apache.lucene.search.Query luceneQuery;

    //jpa query that is created using lucene query and executed
    private javax.persistence.Query jpaQuery;

    int to = 0;

    public LuceneSearch to(int to) {
        this.to = to;
        return this;
    }

    int from = 0;

    public LuceneSearch from(int from) {
        this.from = from;
        return this;
    }

    private ArrayList<String> fields = new ArrayList<>();

    public LuceneSearch onField(String field) {
        this.fields.add(field);
        return this;
    }

    private Class<?> aClass;

    public LuceneSearch onClass(Class<?> aClass) {
        this.aClass = aClass;
        this.fields = new ArrayList<>(); //reset current(old) fields
        return this;
    }

    private TypeOfSearch typeOfSearch;

    public LuceneSearch typeOfSearch(TypeOfSearch typeOfSearch) {
        this.typeOfSearch = typeOfSearch;
        return this;
    }

    private String searchTerm;

    public LuceneSearch searchTerm(String searchTerm) {
        this.searchTerm = searchTerm;
        return this;
    }

}
