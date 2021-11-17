/**
 * @ClassName MapReduceDemo-AA
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月12日12:00 - 周日
 * @Theme
 */
public class DemoTes {
    public static void main(String[] args) {
        //为什么可以连续set呢？
        //因为这些set方法返回的还是对象，所以还是可以点对象的方法。
        new CC.DD().setName("肖华").setAge(18).show2();
        new CC().setId(1).setNumber("flower");

    }
}

interface AA {

}
interface BB extends AA{

}

class CC {
    private String number;
    private int id;

    public int getId() {
        return id;
    }

    public CC setId(int id) {
        this.id = id;
        return this;
    }

    public String getNumber() {
        return number;
    }

    public CC setNumber(String number) {
        this.number = number;
        return this;
    }

    public static class DD{
        private String name = show();

        private String show() {
            System.out.println("show()方法");
            return "show";
        }

        private int age;

        public DD setName(String name){
            this.name = name;
            return this;
        }

        public DD setAge(int age){
            this.age = age;
            return this;
        }

        @Override
        public String toString() {
            return "DD{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }

        public static void show1(){
            System.out.println("DD静态内部类中静态方法");
        }
        public void show2(){
            System.out.println("DD静态内部类中普通方法");
        }
    }
}


