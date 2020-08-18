package RiverDict;

import feign.Param;
import feign.RequestLine;

public interface RiverDictApi {
    @RequestLine("GET /ds/api/v1/datastandard/property/dataElementDetail?tenant={tenant}&uniqueProperty={uniqueProperty}")
    String dataElementDetail(@Param(value = "tenant") String tenant, @Param(value = "uniqueProperty") String uniqueProperty);


    @RequestLine("GET /ds/api/v1/datastandard/dictionary/innerIdentifier/{innerIdentifier}?type={scope}")
    String dataElementDefaultDict(@Param(value = "innerIdentifier") String innerIdentifier,@Param(value = "scope") String scope);


    @RequestLine("GET /ds/api/v1/datastandard/originDictionary?dictionaryMapping={mapping_guid}&instanceGuid={instanceGuid}&dictionaryIdentify={dictionaryIdentify}")
    String dataElementClustomDict(@Param(value = "mapping_guid") String mapping_guid,@Param(value = "instanceGuid") String instanceGuid,@Param(value = "dictionaryIdentify") String dictionaryIdentify);
}
