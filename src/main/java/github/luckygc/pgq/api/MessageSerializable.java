package github.luckygc.pgq.api;

public interface MessageSerializable<M> {

    String serialize(M message);

    M deserialize(String payload);
}
