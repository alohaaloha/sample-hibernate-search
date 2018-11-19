
import org.apache.lucene.search.Query;
import org.hibernate.search.FullTextQuery;
import org.hibernate.search.batchindexing.impl.SimpleIndexingProgressMonitor;
import org.hibernate.search.engine.ProjectionConstants;
import org.hibernate.search.jpa.FullTextEntityManager;
import org.hibernate.search.query.dsl.MustJunction;
import org.hibernate.search.query.dsl.PhraseMatchingContext;
import org.json.JSONObject;

import javax.persistence.EntityManager;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by r.marinkovic on 5/24/2017.
 *
 *  - getting started:    http://hibernate.org/search/documentation/getting-started/
 *
 *  - making queries:     https://docs.jboss.org/hibernate/search/4.5/reference/en-US/html/search-query.html#search-query-querydsl
 *
 *  - mapping:            http://docs.jboss.org/hibernate/stable/search/reference/en-US/html_single/#search-mapping-associated
 *
 *  - usage example:
 *    List<Object> res = luceneSearch
 *      .onClass(RouterInterface.class)
 *      .searchTerm(searchTerm)
 *      .from(from)
 *      .to(to)
 *      .typeOfSearch(LuceneSearch.TypeOfSearch.ALL_WORDS_WITH_WILDCARD)
 *      .onField("name")
 *      .onField("router.ipAddress")
 *      .getSearchResults(); // or .getSearchResultsCount();
 *
 *  - lucene removes characters when indexing:
 *      + - && || ! ( ) { } [ ] ^ \" ~ * ? : \\ /
 *      pattern: .replaceAll("[-+&|!(){}^\"~\\[\\]*?:@$%=,\\/]+", " ")
 *
 */
public class LuceneSearch {

    /**
     * Type of search
     *
     * */
    public enum TypeOfSearch {
        ALL_WORDS_EXACT, ALL_WORDS_WITH_WILDCARD, ANY_WORD
    }

    /**
     * Executes query and returns result data
     *
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
     * Executes query and returns result data with metadata
     *
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
     * Executes query and returns result size
     *
     * */
    public int getSearchResultsCount() {
        //lucine query made using hib query builder
        generateLuceneQuery();
        //create and execute jpa query using lucine query
        if (luceneQuery != null) {
            return fullTextEntityManager.createFullTextQuery(luceneQuery, aClass).getResultSize();
        } else {
            return 0;
        }
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

    /**
     * Returns search term for query builder - with or without wildcard based on start character '='
     *
     * */
    private String addWildcardIfNeeded(String term) {
        if (term.startsWith("=")) {
            term = term.replaceFirst("=", "");
        } else {
            term = term + "*";
        }
        return term;
    }

    /**
     * check if search param is JSON object
     *
     * */
    private boolean isFieldSpecificSearch(String term) {
        try {
            JSONObject jsonObject = new JSONObject(term);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    /**
     * Generate lucene query
     *
     * */
    private Query generateLuceneQuery() {

        fullTextEntityManager = org.hibernate.search.jpa.Search.getFullTextEntityManager(entityManager);
        queryBuilder = fullTextEntityManager.getSearchFactory().buildQueryBuilder().forEntity(aClass).get();

        if (searchTerm != null) {

            System.out.println("Searching " + typeOfSearch + " : " + searchTerm);

            if (!searchTerm.equals("")) {

                if (isFieldSpecificSearch(searchTerm)) {

                    JSONObject jsonObject = new JSONObject(searchTerm);
                    MustJunction mustJunction = null;

                    int init = 0;
                    for (int i = 0; i < jsonObject.keySet().size(); i++) {
                        String field = (String) jsonObject.keySet().toArray()[i];
                        String value = jsonObject.get(field).toString().toLowerCase().trim();

                        if (SearchEntityFields.SEARCH_ENTITY_FIELDS.get(aClass).contains(field) && !value.equals("")) {
                            if (i == init) {
                                //make mustJunction
                                if (value.split(" ").length != 1) {
                                    mustJunction = queryBuilder.bool().must(queryBuilder.phrase().withSlop(10).onField(field).sentence(value).createQuery());
                                } else {
                                    mustJunction = queryBuilder.bool()
                                            .must(queryBuilder.keyword().wildcard().onFields(field).matching(addWildcardIfNeeded(value)).createQuery());
                                }
                            } else {
                                //append
                                if (value.split(" ").length != 1) {
                                    mustJunction.must(queryBuilder.phrase().withSlop(10).onField(field).sentence(value).createQuery());
                                } else {
                                    mustJunction.must(queryBuilder.keyword().wildcard().onFields(field).matching(addWildcardIfNeeded(value)).createQuery());
                                }
                            }
                        } else {
                            init++;
                        }
                    }

                    if (mustJunction != null) {
                        luceneQuery = mustJunction.createQuery();
                    }

                } else {

                    searchTerm = searchTerm.toLowerCase().trim();

                    if (typeOfSearch == TypeOfSearch.ALL_WORDS_EXACT) {

                        PhraseMatchingContext phraseMatchingContext = queryBuilder.phrase().withSlop(10).onField(fields.get(0));
                        if (fields.size() >= 2) {
                            for (int i = 1; i < fields.size(); i++) {
                                phraseMatchingContext.andField(fields.get(i));
                            }
                        }
                        luceneQuery = phraseMatchingContext.sentence(searchTerm).createQuery();

                    } else if (typeOfSearch == TypeOfSearch.ALL_WORDS_WITH_WILDCARD) {

                        String[] terms = searchTerm.split(" ");
                        //make mustJunction
                        MustJunction mustJunction = queryBuilder.bool().must(queryBuilder.keyword().wildcard().onFields(fields.toArray(new String[] {}))
                                .matching(addWildcardIfNeeded(terms[0])).createQuery());
                        for (int i = 1; i < terms.length; i++) {
                            //append
                            mustJunction.must(queryBuilder.keyword().wildcard().onFields(fields.toArray(new String[] {}))
                                    .matching(addWildcardIfNeeded(terms[i])).createQuery());
                        }
                        luceneQuery = mustJunction.createQuery();

                    } else if (typeOfSearch == TypeOfSearch.ANY_WORD) {

                        //option: .keyword().fuzzy()
                        luceneQuery = queryBuilder.keyword().onFields(fields.toArray(new String[] {})).matching(searchTerm).createQuery();

                    }
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

    //entity manager
    private EntityManager entityManager;

    public LuceneSearch(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    private int to = 0;

    public LuceneSearch to(int to) {
        this.to = to;
        return this;
    }

    private int from = 0;

    public LuceneSearch from(int from) {
        this.from = from;
        return this;
    }

    private List<String> fields = new ArrayList<>();

    public LuceneSearch onField(String field) {
        this.fields.add(field);
        return this;
    }

    public LuceneSearch onFields(List<String> fields) {
        this.fields.addAll(fields);
        return this;
    }

    private Class<?> aClass;

    public LuceneSearch onClass(Class<?> aClass) {
        this.aClass = aClass;
        this.fields = new ArrayList<>(); //reset fields if same instance of luceneSearch is used
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
