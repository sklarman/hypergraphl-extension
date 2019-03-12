package org.hypergraphql.query.converters;

import com.fasterxml.jackson.databind.JsonNode;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeCreator;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.hypergraphql.config.schema.QueryFieldConfig;

import org.hypergraphql.datamodel.HGQLSchema;

import org.hypergraphql.config.schema.HGQLVocabulary;
import org.hypergraphql.datafetching.services.SPARQLEndpointService;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class SPARQLServiceConverter {


    private final HGQLSchema schema;

    public SPARQLServiceConverter(HGQLSchema schema) {
        this.schema = schema;
    }

    private final String RDF_TYPE_URI = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";

    private String optionalSTR(String sparqlPattern) {
        final String PATTERN = " OPTIONAL { %s } ";
        return String.format(PATTERN, sparqlPattern);
    }

    private String selectSubquerySTR(String id, String sparqlPattern, String limitOffset) {
        final String PATTERN = "{ SELECT " + varSTR(id) + " WHERE { %s } %s } ";
        return String.format(PATTERN, sparqlPattern, limitOffset);
    }

    private String selectQuerySTR(String whereSTR, String graphID) {
        final String PATTERN = "SELECT * WHERE { %s } ";
        return String.format(PATTERN, graphSTR(graphID, whereSTR));
    }

    private String graphSTR(String graphID, String whereSTR) {
        final String PATTERN = "GRAPH <%s> { %s } ";
        String result = (graphID==null || graphID.equals("")) ? whereSTR : String.format(PATTERN, graphID, whereSTR);
        return result;
    }

    private String valuesSTR(String id, Set<String> input) {
        final String PATTERN = "VALUES " + varSTR(id) + " { %s } ";
        Set<String> uris = new HashSet<>();
        for (String uri : input) uris.add(uriSTR(uri));

        String urisConcat = String.join(" ", uris);

        return String.format(PATTERN, urisConcat);
    }

    private String limitOffsetSTR(JsonNode jsonQuery) {
        JsonNode args = jsonQuery.get("args");
        String limitSTR = "";
        String offsetSTR = "";
        if (args!=null) {
            if (args.has("limit")) limitSTR = limitSTR(args.get("limit").asInt());
            if (args.has("offset")) offsetSTR = offsetSTR(args.get("offset").asInt());
        }
        return limitSTR + offsetSTR;
    }

    private String limitSTR(int no) {
        final String PATTERN = "LIMIT %s ";
        return String.format(PATTERN, no);
    }

    private String offsetSTR(int no) {
        final String PATTERN = "OFFSET %s ";
        return String.format(PATTERN, no);
    }

    private String uriSTR(String uri) {
        final String PATTERN = "<%s>";
        return String.format(PATTERN, uri);
    }

    private String varSTR(String id) {
        final String PATTERN = "?%s";
        return String.format(PATTERN, id);
    }

    private String tripleSTR(String subject, String predicate, String object) {
        final String PATTERN = "%s %s %s . ";
        return String.format(PATTERN, subject, predicate, object);
    }

    private String langFilterSTR(JsonNode field) {
        final String PATTERN = "FILTER (lang(%s) = \"%s\") . ";
        String nodeVar = varSTR(field.get("nodeId").asText());
        JsonNode args = field.get("args");
        String langPattern = (args.has("lang")) ? String.format(PATTERN, nodeVar, args.get("lang").asText()) : "";
        return langPattern;
    }

    private String containsFilterSTR(JsonNode field) {
        final String PATTERN = "FILTER (CONTAINS(LCASE(str(%s)), LCASE(\"%s\"))) . ";
        String nodeVar = varSTR(field.get("nodeId").asText());
        JsonNode args = field.get("args");
        String containsPattern = (args.has("contains")) ? String.format(PATTERN, nodeVar, args.get("contains").asText()) : "";
        return containsPattern;
    }

    private String valueFilterSTR(JsonNode field) {
        final String PATTERN = "FILTER (str(%s) IN (%s)) . ";
        String nodeVar = varSTR(field.get("nodeId").asText());
        JsonNode args = field.get("args");
        String csv = "";
        if (args.has("values")) {
            JsonNode values = args.get("values");
            Iterator<JsonNode> iterator = values.elements();
            while (iterator.hasNext()) {
                if (!csv.equals("")) csv = csv + ", ";
                csv = csv + "\"" + iterator.next().asText() + "\"";
            }
        }
        String inPattern = (!csv.isEmpty()) ? String.format(PATTERN, nodeVar, csv) : "";
        return inPattern;
    }

    private String fieldPattern(String parentId, String nodeId, String predicateURI, String typeURI) {
        String predicateTriple = (parentId.equals("")) ? "" : tripleSTR(varSTR(parentId), uriSTR(predicateURI), varSTR(nodeId));
        String typeTriple = (typeURI.equals("")) ? "" : tripleSTR(varSTR(nodeId), RDF_TYPE_URI, uriSTR(typeURI));
        return predicateTriple + typeTriple;
    }


    public String getSelectQuery(JsonNode jsonQuery, Set<String> input, String rootType) {

        Map<String, QueryFieldConfig> queryFields = schema.getQueryFields();

        Boolean root = (!jsonQuery.isArray() && queryFields.containsKey(jsonQuery.get("name").asText()));

        if (root) {
            if (queryFields.get(jsonQuery.get("name").asText()).type().equals(HGQLVocabulary.HGQL_QUERY_GET_FIELD)) {
                return getSelectRoot_GET(jsonQuery);
            } else {
                return getSelectRoot_GET_BY_ID(jsonQuery);
            }
        } else {
            return getSelectNonRoot((ArrayNode) jsonQuery, input, rootType);
        }
    }

    private String getSelectRoot_GET_BY_ID(JsonNode queryField) {

        Iterator<JsonNode> urisIter = queryField.get("args").get("uris").elements();

        Set<String> uris = new HashSet<>();

        urisIter.forEachRemaining(uri -> uris.add(uri.asText()));

        String targetName = queryField.get("targetName").asText();
        String targetURI = schema.getTypes().get(targetName).getId();
        String graphID = ((SPARQLEndpointService) schema.getQueryFields().get(queryField.get("name").asText()).service()).getGraph();
        String nodeId = queryField.get("nodeId").asText();
        String selectTriple = tripleSTR(varSTR(nodeId), RDF_TYPE_URI, uriSTR(targetURI));
        String valueSTR = valuesSTR(nodeId, uris);

        JsonNode subfields = queryField.get("fields");
        ObjectNode subQuery = getSubQueries(subfields);

        String selectQuery = selectQuerySTR(valueSTR + selectTriple + subQuery.get("clause").asText(), graphID);

        return selectQuery;
    }

    private String getSelectRoot_GET(JsonNode queryField) {

        String targetName = queryField.get("targetName").asText();
        String targetURI = schema.getTypes().get(targetName).getId();
        String graphID = ((SPARQLEndpointService) schema.getQueryFields().get(queryField.get("name").asText()).service()).getGraph();
        String nodeId = queryField.get("nodeId").asText();
        String limitOffsetSTR = limitOffsetSTR(queryField);
        String selectTriple = tripleSTR(varSTR(nodeId), RDF_TYPE_URI, uriSTR(targetURI));
        String rootSubquery = selectSubquerySTR(nodeId, selectTriple, limitOffsetSTR);

        JsonNode subfields = queryField.get("fields");
        ObjectNode whereClause = getSubQueries(subfields);

        String selectQuery = selectQuerySTR(rootSubquery + whereClause.get("clause").asText(), graphID);

        return selectQuery;
    }


    private String getSelectNonRoot(ArrayNode jsonQuery, Set<String> input, String rootType) {


        JsonNode firstField = jsonQuery.elements().next();
        String graphID = ((SPARQLEndpointService) schema.getTypes().get(rootType).getFields().get(firstField.get("name").asText()).getService()).getGraph();
        String parentId = firstField.get("parentId").asText();
        String valueSTR = valuesSTR(parentId, input);

        Iterator<JsonNode> queryFieldsIterator = jsonQuery.elements();

        String whereClause = "";

        while (queryFieldsIterator.hasNext()) {

            JsonNode field = queryFieldsIterator.next();

            ObjectNode subquery = getFieldSubquery(field);

            whereClause += subquery.get("clause").asText();
        }

        String selectQuery = selectQuerySTR(valueSTR + whereClause, graphID);

        return selectQuery;

    }


    private ObjectNode getFieldSubquery(JsonNode fieldJson) {

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode whereNode = mapper.createObjectNode();
        whereNode.put("clause", "");
        boolean optional = true;
        whereNode.put("optional", optional);

        String fieldName = fieldJson.get("name").asText();

        if (HGQLVocabulary.JSONLD.containsKey(fieldName)) return whereNode;

        String fieldURI = schema.getFields().get(fieldName).getId();
        String targetName = fieldJson.get("targetName").asText();
        String parentId = fieldJson.get("parentId").asText();
        String nodeId = fieldJson.get("nodeId").asText();

        String langFilter = langFilterSTR(fieldJson);
        String containsFilter = containsFilterSTR(fieldJson);
        String valueFilter = valueFilterSTR(fieldJson);

        String typeURI = (schema.getTypes().containsKey(targetName)) ? schema.getTypes().get(targetName).getId() : "";

        String fieldPattern = fieldPattern(parentId, nodeId, fieldURI, typeURI);

        JsonNode subfields = fieldJson.get("fields");

        ObjectNode restClauses = getSubQueries(subfields);

        optional = valueFilter.isEmpty() &&  containsFilter.isEmpty() && restClauses.get("optional").asBoolean();

        String whereClause = (optional) ? optionalSTR(fieldPattern + langFilter + restClauses.get("clause").asText()) : fieldPattern + langFilter + containsFilter + valueFilter + restClauses.get("clause").asText();

        whereNode.put("clause", whereClause);
        whereNode.put("optional", optional);

        return whereNode;
    }


    private ObjectNode getSubQueries(JsonNode subfields) {

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode whereNode = mapper.createObjectNode();
        whereNode.put("clause", "");
        boolean optional = true;
        whereNode.put("optional", optional);

        if (subfields.isNull()) return whereNode;

        Iterator<JsonNode> queryFieldsIterator = subfields.elements();

        String whereClause = "";

        while (queryFieldsIterator.hasNext()) {

            JsonNode field = queryFieldsIterator.next();
            ObjectNode subqueryField = getFieldSubquery(field);
            if (!subqueryField.get("optional").asBoolean()) optional = false;
            whereClause += subqueryField.get("clause").asText();
        }

        whereNode.put("clause", whereClause);
        whereNode.put("optional", optional);

        return whereNode;

    }


}
