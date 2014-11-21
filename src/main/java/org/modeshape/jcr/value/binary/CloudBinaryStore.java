package org.modeshape.jcr.value.binary;

import org.jclouds.ContextBuilder;
import org.jclouds.aws.s3.AWSS3ProviderMetadata;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.io.Payload;
import org.jclouds.io.payloads.InputStreamPayload;
import org.joda.time.DateTime;
import org.modeshape.jcr.JcrI18n;
import org.modeshape.jcr.value.BinaryKey;
import org.modeshape.jcr.value.BinaryValue;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

/**
 * A {@link org.modeshape.jcr.value.binary.BinaryStore} implementation that use a cloud store for persisting binary values.
 * <p>
 *     This binary store implementation establishes a connection to the specified cloud service. The following cloud services are supported :
 *     <ul>
 *         <li>aws-s3</li>
 *     </ul>
 * </p>
 *<br>
 * Created at 04/11/2014 09:49.<br>
 *
 * @author Bastien
 *
 */
public class CloudBinaryStore extends AbstractBinaryStore {


    private String provider;
    private String container;
    private String accessKey;
    private String secretKey;

    BlobStoreContext context;
    BlobStore blobStore;

    private static final String MARK_UNUSED_STMT_KEY = "mark_unused";
    private static final String MARK_USED_STMT_KEY = "mark_used";
    private static final String USED = "used";

    private FileSystemBinaryStore cache;

    public CloudBinaryStore() {
        this.cache = TransientBinaryStore.get();
    }

    public CloudBinaryStore(String provider, String container, String accessKey, String secretKey) {
        this.cache = TransientBinaryStore.get();
        this.provider = provider;
        this.container = container;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    @Override
    public void start() {
        context = ContextBuilder.newBuilder(new AWSS3ProviderMetadata())
                .credentials(accessKey, secretKey)
                .buildView(BlobStoreContext.class);
        blobStore = context.getBlobStore();
        if (!blobStore.containerExists(container)) {
            blobStore.createContainerInLocation(null, container);
        }
    }

    @Override
    public void shutdown() {
        context.close();
    }

    @Override
    protected String getStoredMimeType(BinaryValue binaryValue) throws BinaryStoreException {
        if (!blobStore.blobExists(container, binaryValue.getKey().toString())) {
            throw new BinaryStoreException(JcrI18n.unableToFindBinaryValue.text(binaryValue.getKey(), container));
        }
        try {
            return blobStore.getBlob(container, binaryValue.getKey().toString()).getMetadata().getContentMetadata().getContentType();
        } catch (Exception e) {
            throw new BinaryStoreException(JcrI18n.unableToFindBinaryValue.text(binaryValue.getKey(), container));
        }
    }

    @Override
    protected void storeMimeType(BinaryValue binaryValue, String mimeType) throws BinaryStoreException {
        if (blobStore.blobExists(container, binaryValue.getKey().toString())) {
            blobStore.getBlob(container, binaryValue.getKey().toString()).getMetadata().getContentMetadata().setContentType(mimeType);
        } else {
            throw new BinaryStoreException(JcrI18n.unableToFindBinaryValue.text(binaryValue.getKey(), container));
        }
    }

    @Override
    public void storeExtractedText(BinaryValue source, String extractedText) throws BinaryStoreException {

    }

    @Override
    public String getExtractedText(BinaryValue source) throws BinaryStoreException {
        return "";
    }

    @Override
    public BinaryValue storeValue(InputStream stream, boolean markAsUnused) throws BinaryStoreException {
        return storeValue(stream, null, markAsUnused);
    }

    @Override
    public BinaryValue storeValue(InputStream stream, String hint, boolean markAsUnused) throws BinaryStoreException {
        final BinaryValue temp = cache.storeValue(stream, markAsUnused);

        BinaryKey key = new BinaryKey(temp.getKey().toString());
        String k = key.toString();
        try {
            if (hint != null) {
                if (!blobStore.directoryExists(container, hint)) {
                    blobStore.createDirectory(container, hint);
                }
                k = hint + "/" + key.toString();
            }
            if (blobStore.blobExists(container, k)) {
                return new StoredBinaryValue(this, key, temp.getSize());
            }
            Payload payload = new InputStreamPayload(temp.getStream());
            Blob blob = blobStore.blobBuilder(k)
                    .payload(payload)
                    .contentLength(temp.getSize())
                    .build();

            blobStore.putBlob(container, blob);
            return new StoredBinaryValue(this, key, temp.getSize());
        } catch (Exception e) {
            throw new BinaryStoreException(e);
        } finally {
            cache.markAsUnused(temp.getKey());
        }
    }

    @Override
    public InputStream getInputStream(BinaryKey key) throws BinaryStoreException {
        try {
            InputStream in = blobStore.getBlob(container, key.toString()).getPayload().openStream();
            if (in == null) {
                throw new BinaryStoreException("");
            }
            return in;
        } catch (IOException e) {
            throw new BinaryStoreException(e);
        }
    }

    @Override
    public void markAsUsed(Iterable<BinaryKey> keys) throws BinaryStoreException {
        try {
            for (BinaryKey k : keys) {
                blobStore.getBlob(container, k.toString()).getMetadata().getUserMetadata().put(USED, MARK_USED_STMT_KEY);
                blobStore.getBlob(container, k.toString()).getMetadata().getContentMetadata().setExpires(null);
            }
        } catch (Exception e) {
            throw new BinaryStoreException(e);
        }
    }

    @Override
    public void markAsUnused(Iterable<BinaryKey> keys) throws BinaryStoreException {
        try {
            for (BinaryKey k : keys) {
                blobStore.getBlob(container, k.toString()).getMetadata().getUserMetadata().put(USED, MARK_UNUSED_STMT_KEY);
                DateTime dateTime = new DateTime();
                dateTime.plusMonths(1);
                blobStore.getBlob(container, k.toString()).getMetadata().getContentMetadata().setExpires(dateTime.toDate());
            }
        } catch (Exception e) {
            throw new BinaryStoreException(e);
        }
    }

    @Override
    public void removeValuesUnusedLongerThan(long minimumAge, TimeUnit unit) throws BinaryStoreException {
        String marker = null;
        while (true) {
            PageSet<StorageMetadata> set = (PageSet<StorageMetadata>) blobStore.list(container,
                    new ListContainerOptions().afterMarker(marker));
            for (StorageMetadata sm : set) {
                if (new DateTime(sm.getLastModified()).plus(TimeUnit.DAYS.convert(minimumAge, unit)).toDate().before(new DateTime().toDate())) {
                    blobStore.removeBlob(container, sm.getName());
                }
            }
            marker = set.getNextMarker();
            if (marker == null) {
                break;
            }
        }
    }


    @Override
    public Iterable<BinaryKey> getAllBinaryKeys() throws BinaryStoreException {
        return null;
    }

    public void setProvider(String provider) {
        this.provider = provider;
    }

    public void setContainer(String container) {
        this.container = container;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }
}
