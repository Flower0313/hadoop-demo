package demo01;

/**
 * @ClassName MapReduceDemo-AA
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月13日22:14 - 周一
 * @Describe
 */
public class AA {
    public static void main(String[] args) {
        //创建CC类的时候，
        CC cc = new CC();
        cc.show();
    }
}

class CC {
    private BB bb = new BB();

    public void show(){
        bb.toString();
    }

    class BB extends Object{


        @Override
        public String toString() {
            System.out.println("i am BB");
            return "BB";
        }
    }
}
