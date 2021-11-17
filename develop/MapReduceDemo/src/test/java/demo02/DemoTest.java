package demo02;

/**
 * @ClassName MapReduceDemo-DemoTest
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月14日12:13 - 周二
 * @Describe
 */
public class DemoTest {
    public static void main(String[] args) {
        CC bb = new CC();
        bb.show(new DD(){
            @Override
            public void run() {
                System.out.println("牛逼");
            }
        });
    }
}

class AA{
    private CC cc;

    public AA() {
    }

    public AA(CC c, String name){
        try {
            cc = (CC) c.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        c.name = name;
    }
}

class BB extends AA{
    public BB() {
        super();
    }

    private CC cc;
    //将c传给AA的时候是传的地址，通俗点说只是将c拿到AA中去改造的
    public BB(CC c,String name) {
        super(c,name);
        this.cc = c;
    }

    CC cc(String name){
        return null;
    }

    public void show(DD dd){
    }

}

class CC implements Cloneable{
    public String name;

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    public void show(DD dd){
        //dd.run();
        System.out.println("调用");
    }
}

interface DD{
    void run();
}
class EE{
    private String xiao = new String("s");
    public int xiao(){
        System.out.println("ss");
        return 1;
    }
}

