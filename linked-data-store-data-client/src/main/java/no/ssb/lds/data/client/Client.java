package no.ssb.lds.data.client;

import no.ssb.lds.data.common.model.GSIMDataset;

import java.nio.channels.ReadableByteChannel;

/**
 * A simple client API to upload GSIM Dataset data.
 */
public interface Client {

    GSIMDataset getDataset(String id);

    void putData(GSIMDataset dataset, ReadableByteChannel data, String mimeType);

    GSIMDataset putData(ReadableByteChannel data, String mimeType);

    Object getData(GSIMDataset dataset, String filter, String order);



}
