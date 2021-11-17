package builderMode;

/**
 * @ClassName MapReduceDemo-OuterBuilder
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年10月10日18:13 - 周日
 * @Describe
 */
public class OuterBuilder {
    private InnerDescriptor inner;
    private final String name = null;
    private final int age = 0;


    private OuterBuilder(String name) {
        this.inner = new InnerDescriptor(name);
    }

    public static OuterBuilder newBuilder(final String name) {
        return new OuterBuilder(name);
    }

    public OuterBuilder setName(String name) {
        inner.setName(name);
        return this;
    }

    public OuterBuilder setAge(int age) {
        inner.age = age;
        return this;
    }


    //内部类
    public static class InnerDescriptor {
        private String name;
        private int age;

        public InnerDescriptor(String name) {
            this.name = name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }


}
