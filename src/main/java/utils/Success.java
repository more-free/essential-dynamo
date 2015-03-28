package utils;

public class Success<T> extends Try<T> {
    private T t;

    public Success() {
        this.t = null;
    }

    public Success(T t) {
        this.t = t;
    }

    public T get() {
        return t;
    }
}
