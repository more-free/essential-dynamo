package utils;

import java.util.function.Consumer;

public class Try<T> {
    protected Try() { }

    public Try onSuccess(Consumer<T> consumer) {
        if(this instanceof Success) {
            T t  = ((Success<T>) this).get();
            consumer.accept(t);
        }

        return this;
    }

    public Try onFailure(Consumer<Throwable> consumer) {
        if(this instanceof Failure) {
            Throwable e = ((Failure) this).get();
            consumer.accept(e);
        }

        return this;
    }
}


