package utils;

public class Failure<T> extends Try<T> {
    private Throwable e;

    public Failure() {
        this.e = null;
    }

    public Failure(Throwable e) {
        this.e = e;
    }

    public Throwable get() {
        return e;
    }
}
