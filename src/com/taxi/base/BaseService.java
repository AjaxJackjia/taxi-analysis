package com.taxi.base;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;


public class BaseService
{
    public Response buildResponse(Object p_result, String p_mimeType, java.util.Date p_expires)
    {
        ResponseBuilder builder = Response.ok(p_result, p_mimeType);
        if (p_expires != null)
        {
            builder.expires(p_expires);
        }
        return builder.build();
    }
}