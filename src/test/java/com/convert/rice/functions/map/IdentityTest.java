package com.convert.rice.functions.map;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;

import org.easymock.EasyMockSupport;
import org.junit.Test;

import com.convert.rice.DataPoints;

public class IdentityTest extends EasyMockSupport {

    @Test
    public void testApply() {
        DataPoints dps = createMock(DataPoints.class);
        List<DataPoints> result = new Identity().apply(dps);
        assertEquals(Collections.singletonList(dps), result);
    }
}
