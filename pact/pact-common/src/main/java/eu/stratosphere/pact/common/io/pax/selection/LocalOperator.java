package eu.stratosphere.pact.common.io.pax.selection;

import eu.stratosphere.pact.common.type.Key;

/**
 * Operators for the local predicates.
 *
 * @author Andreas Kunft
 */
public enum LocalOperator {
    EQUAL(new ILocalOperator<Key>() {
        @Override
        public boolean evaluate(Key columnValue, Key conditionValue) {
            return columnValue.equals(conditionValue);
        }

        @Override
        public boolean evaluateHighLow(Key low, Key high, Key conditionValue) {
            return low.compareTo(conditionValue) <= 0 && high.compareTo(conditionValue) >= 0;
        }
    }) {
        @Override
        LocalOperator invert() {
            return NOT_EQUAL;
        }

        @Override
        public String toString() {
            return "=";
        }
    }, NOT_EQUAL(new ILocalOperator<Key>() {

        public boolean evaluate(Key columnValue, Key conditionValue) {
            return !columnValue.equals(conditionValue);
        }

        @Override
        public boolean evaluateHighLow(Key low, Key high, Key conditionValue) {
            return !(low.compareTo(conditionValue) <= 0 && high.compareTo(conditionValue) >= 0);
        }
    }) {
        @Override
        LocalOperator invert() {
            return EQUAL;
        }

        @Override
        public String toString() {
            return "!=";
        }
    }, LESS_THEN(new ILocalOperator<Key>() {
        @Override
        public boolean evaluate(Key columnValue, Key conditionValue) {
            return columnValue.compareTo(conditionValue) < 0;
        }

        @Override
        public boolean evaluateHighLow(Key low, Key high, Key conditionValue) {
            return low.compareTo(conditionValue) < 0 || high.compareTo(conditionValue) < 0;
        }
    }) {
        @Override
        LocalOperator invert() {
            return GREATER_EQUAL_THEN;
        }

        @Override
        public String toString() {
            return "<";
        }
    }, GREATER_THEN(new ILocalOperator<Key>() {
        @Override
        public boolean evaluate(Key columnValue, Key conditionValue) {
            return columnValue.compareTo(conditionValue) > 0;
        }

        @Override
        public boolean evaluateHighLow(Key low, Key high, Key conditionValue) {
            return low.compareTo(conditionValue) > 0 || high.compareTo(conditionValue) > 0;
        }
    }) {
        @Override
        LocalOperator invert() {
            return LESS_EQUAL_THEN;
        }

        @Override
        public String toString() {
            return ">";
        }
    }, GREATER_EQUAL_THEN(new ILocalOperator<Key>() {
        @Override
        public boolean evaluate(Key columnValue, Key conditionValue) {
            return columnValue.compareTo(conditionValue) >= 0;
        }

        @Override
        public boolean evaluateHighLow(Key low, Key high, Key conditionValue) {
            return low.compareTo(conditionValue) >= 0 || high.compareTo(conditionValue) >= 0;
        }
    }) {
        @Override
        LocalOperator invert() {
            return LESS_THEN;
        }

        @Override
        public String toString() {
            return ">=";
        }
    }, LESS_EQUAL_THEN(new ILocalOperator<Key>() {
        @Override
        public boolean evaluate(Key columnValue, Key conditionValue) {
            return columnValue.compareTo(conditionValue) <= 0;
        }

        @Override
        public boolean evaluateHighLow(Key low, Key high, Key conditionValue) {
            return low.compareTo(conditionValue) <= 0 || high.compareTo(conditionValue) <= 0;
        }
    }) {
        @Override
        LocalOperator invert() {
            return GREATER_THEN;
        }

        @Override
        public String toString() {
            return "<=";
        }
    },

    /**
     * THIS TYPE IS ONLY FOR CREATION PURPOSES!
     * <p/>
     * WHEN A PREDICATE IS BUILT, ALL BETWEEN OPERATORS ARE REPLACED >= & <=.
     */
    BETWEEN(new ILocalOperator<Key>() {
        @Override
        public boolean evaluate(Key columnValue, Key conditionValue) {
            throw new UnsupportedOperationException("This type only exists for predicate creation.");
        }

        @Override
        public boolean evaluateHighLow(Key low, Key high, Key conditionValue) {
            throw new UnsupportedOperationException("This type only exists for predicate creation.");
        }
    }) {
        @Override
        public String toString() {
            throw new UnsupportedOperationException("This type only exists for predicate creation.");
        }

        @Override
        LocalOperator invert() {
            throw new UnsupportedOperationException("This type only exists for predicate creation.");
        }
    };

    /**
     * The condition to be evaluated.
     */
    public final ILocalOperator<Key> condition;

    private LocalOperator(ILocalOperator<Key> condition) {
        this.condition = condition;
    }

    @Override
    public abstract String toString();

    /**
     * Returns the negated version of the operator.
     *
     * @return the negated version of the operator.
     */
    abstract LocalOperator invert();
}
