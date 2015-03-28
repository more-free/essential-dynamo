package rpc.thrift;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class VersionedStorageUnit implements Serializable {
    private String version;
    private byte [] value;

    public VersionedStorageUnit(Versioned versioned) {
        this.version = versioned.version;
        this.value = versioned.value.array();
    }

    public VersionedStorageUnit(String version, byte [] value) {
        this.version = version;
        this.value = value;
    }

    public Versioned toVersioned() {
        return new Versioned(ByteBuffer.wrap(value), version);
    }

    public String getVersion() {
        return version;
    }

    public byte [] getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "version = " + version + ", value = " + new String(value);
    }
}