package com.anmol.projects;

public class HashFunction<T> {

    private final Integer number1;

    public HashFunction(Integer givenNumber) {
        this.number1 = givenNumber;

    }

    public Integer getHash(T object) {
        int objectHash = object.hashCode();
        return Math.abs((int) ((long) objectHash * number1) % Integer.MAX_VALUE);


    }

    @Override
    public String toString() {
        return "HashFunction{" +
                "n=" + number1 +
                '}';
    }
}
