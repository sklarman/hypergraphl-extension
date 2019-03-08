package org.hypergraphql.datafetching;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.BasicCredentialsProvider;
//import org.apache.http.impl.client.HttpClients;
import org.apache.jena.atlas.web.auth.PreemptiveBasicAuthenticator;
import org.apache.jena.atlas.web.auth.ScopedAuthenticator;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.web.HttpOp;
import org.apache.jena.sparql.engine.http.QueryEngineHTTP;
import org.apache.log4j.Logger;
import org.hypergraphql.datafetching.services.SPARQLEndpointService;
import org.hypergraphql.datamodel.HGQLSchema;
import org.hypergraphql.query.converters.SPARQLServiceConverter;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;


public class SPARQLEndpointExecution implements Callable<SPARQLExecutionResult> {

    protected JsonNode query;
    protected Set<String> inputSubset;
    protected Set<String> markers;
    protected SPARQLEndpointService sparqlEndpointService;
    protected HGQLSchema schema ;
    protected Logger logger = Logger.getLogger(SPARQLEndpointExecution.class);
    protected String rootType;


    public SPARQLEndpointExecution(JsonNode query, Set<String> inputSubset, Set<String> markers, SPARQLEndpointService sparqlEndpointService, HGQLSchema schema, String rootType) {
        this.query = query;
        this.inputSubset = inputSubset;
        this.markers = markers;
        this.sparqlEndpointService = sparqlEndpointService;
        this.schema = schema;
        this.rootType=rootType;


    }


    @Override

    public SPARQLExecutionResult call() throws Exception {
        Map<String, Set<String>> resultSet = new HashMap<>();
        for (String marker : markers) {
            resultSet.put(marker, new HashSet<>());
        }

        Model unionModel = ModelFactory.createDefaultModel();


        SPARQLServiceConverter converter = new SPARQLServiceConverter(schema);
        String sparqlQuery = converter.getSelectQuery(query, inputSubset, rootType);
        logger.info(sparqlQuery);

        PreemptiveBasicAuthenticator authenticator = null;
        try {
            authenticator = new PreemptiveBasicAuthenticator(new ScopedAuthenticator(new URI(this.sparqlEndpointService.getUrl()), this.sparqlEndpointService.getUser(), this.sparqlEndpointService.getPassword().toCharArray()));
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        System.out.println(sparqlQuery);
        QueryExecution qe = QueryExecutionFactory.sparqlService(this.sparqlEndpointService.getUrl(), sparqlQuery, authenticator);


        ResultSet results = qe.execSelect();

        while (results.hasNext()) {
            QuerySolution solution = results.next();

            for (String marker : markers) {
                if (solution.contains(marker)) resultSet.get(marker).add(solution.get(marker).asResource().getURI());

            }

            Model model = this.sparqlEndpointService.getModelFromResults(query, solution, schema);
            unionModel.add(model);


        }

        SPARQLExecutionResult sparqlExecutionResult = new SPARQLExecutionResult(resultSet, unionModel);
        logger.info(sparqlExecutionResult);

        return sparqlExecutionResult;
    }

}

