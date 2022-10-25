package som.make.mock.calcite.mysql.rules;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.tools.RelBuilderFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ImmutableMysqlFilterRule {

    private ImmutableMysqlFilterRule() {
    }

    static final class Config implements MysqlFilterRule.Config {

        private final RelBuilderFactory relBuilderFactory;
        private final @Nullable String description;
        private final RelRule.OperandTransform operandSupplier;

        private Config(Builder builder) {
            this.description = builder.description;
            if (builder.relBuilderFactory != null) {
                initShim.withRelBuilderFactory(builder.relBuilderFactory);
            }
            if (builder.operandSupplier != null) {
                initShim.withOperandSupplier(builder.operandSupplier);
            }
            this.relBuilderFactory = initShim.relBuilderFactory();
            this.operandSupplier = initShim.operandSupplier();
            this.initShim = null;
        }

        private Config(RelBuilderFactory relBuilderFactory, @Nullable String description, RelRule.OperandTransform operandSupplier) {
            this.relBuilderFactory = relBuilderFactory;
            this.description = description;
            this.operandSupplier = operandSupplier;
        }

        private static final byte STAGE_INITIALIZING = -1;
        private static final byte STAGE_UNINITIALIZED = 0;
        private static final byte STAGE_INITIALIZED = 1;
        @SuppressWarnings("Immutable")
        private transient volatile InitShim initShim = new InitShim();

        private final class InitShim {
            private byte relBuilderFactoryBuildStage = STAGE_UNINITIALIZED;
            private RelBuilderFactory relBuilderFactory;

            RelBuilderFactory relBuilderFactory() {
                if (relBuilderFactoryBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
                if (relBuilderFactoryBuildStage == STAGE_UNINITIALIZED) {
                    relBuilderFactoryBuildStage = STAGE_INITIALIZING;
                    this.relBuilderFactory = Objects.requireNonNull(relBuilderFactoryInitialize(), "relBuilderFactory");
                    relBuilderFactoryBuildStage = STAGE_INITIALIZED;
                }
                return this.relBuilderFactory;
            }

            void withRelBuilderFactory(RelBuilderFactory relBuilderFactory) {
                this.relBuilderFactory = relBuilderFactory;
                relBuilderFactoryBuildStage = STAGE_INITIALIZED;
            }

            private byte operandSupplierBuildStage = STAGE_UNINITIALIZED;
            private RelRule.OperandTransform operandSupplier;

            RelRule.OperandTransform operandSupplier() {
                if (operandSupplierBuildStage == STAGE_INITIALIZING) throw new IllegalStateException(formatInitCycleMessage());
                if (operandSupplierBuildStage == STAGE_UNINITIALIZED) {
                    operandSupplierBuildStage = STAGE_INITIALIZING;
                    this.operandSupplier = Objects.requireNonNull(operandSupplierInitialize(), "operandSupplier");
                    operandSupplierBuildStage = STAGE_INITIALIZED;
                }
                return this.operandSupplier;
            }

            void withOperandSupplier(RelRule.OperandTransform operandSupplier) {
                this.operandSupplier = operandSupplier;
                operandSupplierBuildStage = STAGE_INITIALIZED;
            }

            private String formatInitCycleMessage() {
                List<String> attributes = new ArrayList<>();
                if (relBuilderFactoryBuildStage == STAGE_INITIALIZING) attributes.add("relBuilderFactory");
                if (operandSupplierBuildStage == STAGE_INITIALIZING) attributes.add("operandSupplier");
                return "Cannot build Config, attribute initializers form cycle " + attributes;
            }
        }

        private RelBuilderFactory relBuilderFactoryInitialize() {
            return MysqlFilterRule.Config.super.relBuilderFactory();
        }

        private RelRule.OperandTransform operandSupplierInitialize() {
            return MysqlFilterRule.Config.super.operandSupplier();
        }

        @Override
        public RelRule.Config withRelBuilderFactory(RelBuilderFactory factory) {
            if (this.relBuilderFactory == factory) return this;
            RelBuilderFactory newValue = Objects.requireNonNull(factory, "relBuilderFactory");
            return new Config(newValue, this.description, this.operandSupplier);
        }

        @Override
        public RelBuilderFactory relBuilderFactory() {
            InitShim shim = this.initShim;
            return shim != null
                    ? shim.relBuilderFactory()
                    : this.relBuilderFactory;
        }

        @Nullable
        @Override
        public @org.checkerframework.checker.nullness.qual.Nullable String description() {
            return description;
        }

        @Override
        public RelRule.Config withDescription(@org.checkerframework.checker.nullness.qual.Nullable String description) {
            if (Objects.equals(this.description, description)) return this;
            return new Config(this.relBuilderFactory, description, this.operandSupplier);
        }

        @Override
        public RelRule.Config withOperandSupplier(RelRule.OperandTransform transform) {
            if (this.operandSupplier == transform) return this;
            RelRule.OperandTransform newValue = Objects.requireNonNull(transform, "operandSupplier");
            return new Config(this.relBuilderFactory, this.description, newValue);
        }

        @Override
        public RelRule.OperandTransform operandSupplier() {
            InitShim shim = this.initShim;
            return shim != null
                    ? shim.operandSupplier()
                    : this.operandSupplier;
        }

        /**
         * This instance is equal to all instances of {@code Config} that have equal attribute values.
         * @return {@code true} if {@code this} is equal to {@code another} instance
         */
        @Override
        public boolean equals(@Nullable Object another) {
            if (this == another) return true;
            return another instanceof Config
                    && equalTo((Config) another);
        }

        private boolean equalTo(Config another) {
            return relBuilderFactory.equals(another.relBuilderFactory)
                    && Objects.equals(description, another.description)
                    && operandSupplier.equals(another.operandSupplier);
        }

        /**
         * Computes a hash code from attributes: {@code relBuilderFactory}, {@code description}, {@code operandSupplier}.
         * @return hashCode value
         */
        @Override
        public int hashCode() {
            @Var int h = 5381;
            h += (h << 5) + relBuilderFactory.hashCode();
            h += (h << 5) + Objects.hashCode(description);
            h += (h << 5) + operandSupplier.hashCode();
            return h;
        }

        /**
         * Prints the immutable value {@code Config} with attribute values.
         * @return A string representation of the value
         */
        @Override
        public String toString() {
            return MoreObjects.toStringHelper("Config")
                    .omitNullValues()
                    .add("relBuilderFactory", relBuilderFactory)
                    .add("description", description)
                    .add("operandSupplier", operandSupplier)
                    .toString();
        }

        /**
         * Creates an immutable copy of a {@link MysqlFilterRule.Config} value.
         * Uses accessors to get values to initialize the new immutable instance.
         * If an instance is already immutable, it is returned as is.
         * @param instance The instance to copy
         * @return A copied immutable Config instance
         */
        public static Config copyOf(MysqlFilterRule.Config instance) {
            if (instance instanceof Config) {
                return (Config) instance;
            }
            return Config.builder()
                    .from(instance)
                    .build();
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {

            private @Nullable RelBuilderFactory relBuilderFactory;
            private @Nullable String description;
            private @Nullable RelRule.OperandTransform operandSupplier;

            private Builder() {
            }

            @CanIgnoreReturnValue
            public final Builder from(RelRule.Config instance) {
                Objects.requireNonNull(instance, "instance");
                from((Object) instance);
                return this;
            }

            @CanIgnoreReturnValue
            public final Builder from(MysqlFilterRule.Config instance) {
                Objects.requireNonNull(instance, "instance");
                from((Object) instance);
                return this;
            }

            private void from(Object object) {
                if (object instanceof RelRule.Config instance) {
                    withRelBuilderFactory(instance.relBuilderFactory());
                    withOperandSupplier(instance.operandSupplier());
                    @Nullable String descriptionValue = instance.description();
                    if (descriptionValue != null) {
                        withDescription(descriptionValue);
                    }
                }
            }

            @CanIgnoreReturnValue
            public final Builder withRelBuilderFactory(RelBuilderFactory relBuilderFactory) {
                this.relBuilderFactory = Objects.requireNonNull(relBuilderFactory, "relBuilderFactory");
                return this;
            }

            @CanIgnoreReturnValue
            public final Builder withOperandSupplier(RelRule.OperandTransform operandSupplier) {
                this.operandSupplier = Objects.requireNonNull(operandSupplier, "operandSupplier");
                return this;
            }

            @CanIgnoreReturnValue
            public final Builder withDescription(@Nullable String description) {
                this.description = description;
                return this;
            }

            public Config build() {
                return new Config(this);
            }

        }
    }

}
