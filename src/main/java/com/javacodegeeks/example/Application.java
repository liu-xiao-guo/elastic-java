package com.javacodegeeks.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Application {

    //The config parameters for the connection
    private static final String HOST = "localhost";
    private static final int PORT_ONE = 9200;
    private static final int PORT_TWO = 9201;
    private static final String SCHEME = "http";

    private static RestHighLevelClient restHighLevelClient;
    private static ObjectMapper objectMapper = new ObjectMapper();

    private static final String INDEX = "persondata";
    private static final String TYPE = "_doc";

    /**
     * Implemented Singleton pattern here
     * so that there is just one connection at a time.
     * @return RestHighLevelClient
     */
    private static synchronized RestHighLevelClient makeConnection() {

        if(restHighLevelClient == null) {
            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost(HOST, PORT_ONE, SCHEME),
                            new HttpHost(HOST, PORT_TWO, SCHEME)));
        }

        return restHighLevelClient;
    }

    private static synchronized void closeConnection() throws IOException {
        restHighLevelClient.close();
        restHighLevelClient = null;
    }

    private static Person insertPerson(Person person) {

        person.setPersonId(UUID.randomUUID().toString());
        Map<String, Object> dataMap = new HashMap<String, Object>();
        dataMap.put("name", person.getName());
        dataMap.put("number", person.getNumber());
        IndexRequest indexRequest = new IndexRequest(INDEX)
                .id(person.getPersonId()).source(dataMap);
        try {
            IndexResponse response = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        } catch(ElasticsearchException e) {
            e.getDetailedMessage();
        } catch (java.io.IOException ex){
            ex.getLocalizedMessage();
        }

        /*
        // The following is another way to do it
        // More information https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.3/java-rest-high-document-index.html
        String id = UUID.randomUUID().toString();
        person.setPersonId(id);
        IndexRequest request = new IndexRequest(INDEX);
        request.id(id);
        String jsonString = "{" +
                "\"name\":" + "\"" + person.getName() + "\"" +
                "}";

        System.out.println("jsonString: " + jsonString);
        request.source(jsonString, XContentType.JSON);
        try {
            IndexResponse response = restHighLevelClient.index(request, RequestOptions.DEFAULT);

        } catch(ElasticsearchException e) {
            e.getDetailedMessage();
        } catch (java.io.IOException ex){
            ex.getLocalizedMessage();
        }
         */

        return person;
    }

    private static Person getPersonById(String id){

        GetRequest getPersonRequest = new GetRequest(INDEX, id);
        GetResponse getResponse = null;
        try {
            getResponse = restHighLevelClient.get(getPersonRequest, RequestOptions.DEFAULT);
        } catch (java.io.IOException e){
            e.getLocalizedMessage();
        }
        return getResponse != null ?
                objectMapper.convertValue(getResponse.getSourceAsMap(), Person.class) : null;
    }

    private static SearchResponse searchAll(){
        SearchRequest searchRequest = new SearchRequest(INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        final SearchRequest source = searchRequest.source(searchSourceBuilder);
        SearchResponse response = null;
        try {
            response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (java.io.IOException e){
            e.getLocalizedMessage();
        }

        return response;
    }

    private static SearchResponse searchTerm(){
        SearchRequest searchRequest = new SearchRequest(INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery("name.keyword", "张三"));
        final SearchRequest source = searchRequest.source(searchSourceBuilder);
        SearchResponse response = null;
        try {
            response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (java.io.IOException e){
            e.getLocalizedMessage();
        }

        return response;
    }

    private static SearchResponse matchQuery() {
        Map<String, String> params = Collections.emptyMap();

        /*
        String queryString = "{" +
             "   \"size\": 4, " +
             "   \"query\": {" +
             "   \"match\": {" +
             "   \"name\": \"张三\" " +
            "}";
        */

        SearchRequest searchRequest = new SearchRequest(INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("name", "张三"));
        final SearchRequest source = searchRequest.source(searchSourceBuilder);
        SearchResponse response = null;
        try {
            response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (java.io.IOException e){
            e.getLocalizedMessage();
        }

        return response;
    }


    private static Person updatePersonById(String id, Person person){
        UpdateRequest updateRequest = new UpdateRequest(INDEX, id)
                .fetchSource(true);    // Fetch Object after its update
        try {
            String personJson = objectMapper.writeValueAsString(person);
            updateRequest.doc(personJson, XContentType.JSON);
            UpdateResponse updateResponse = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
            return objectMapper.convertValue(updateResponse.getGetResult().sourceAsMap(), Person.class);
        }catch (JsonProcessingException e){
            e.getMessage();
        } catch (java.io.IOException e){
            e.getLocalizedMessage();
        }
        System.out.println("Unable to update person");
        return null;
    }


    private static void deletePersonById(String id) {
        DeleteRequest deleteRequest = new DeleteRequest(INDEX, TYPE, id);
        try {
            DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        } catch (java.io.IOException e) {
            e.getLocalizedMessage();
        }
    }


    public static void main(String[] args) throws IOException {

        makeConnection();

        Person person = new Person();
        person.setName("张三");
        System.out.println("Inserting a new Person with name " + person.getName());
        person.setNumber("111111");
        person = insertPerson(person);
        System.out.println("Person inserted --> " + person);


        person = new Person();
        person.setName("姚明");
        System.out.println("Inserting a new Person with name " + person.getName());
        person.setNumber("222222");
        person = insertPerson(person);
        System.out.println("Person inserted --> " + person);


        person.setName("李四");
        System.out.println("Changing name to " + person.getName());
        updatePersonById(person.getPersonId(), person);
        System.out.println("Person updated  --> " + person);


        System.out.println("Searching for all documents");
        SearchResponse response = searchAll();
        System.out.println(response);

        System.out.println("Searching for a term");
        response = searchTerm();
        System.out.println(response);

        System.out.println("Match a query");
        response = matchQuery();
        System.out.println(response);

        System.out.println("Getting 李四");
        Person personFromDB = getPersonById(person.getPersonId());
        System.out.println("Person from DB  --> " + personFromDB);



        System.out.println("Deleting " + person.getName());
        deletePersonById(personFromDB.getPersonId());
        System.out.println("Person " + person.getName() + " deleted!");


        closeConnection();
    }
}
