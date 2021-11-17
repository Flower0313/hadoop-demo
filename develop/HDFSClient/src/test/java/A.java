/**
 * @ClassName HDFSClient-A
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年9月05日22:43 - 周日
 * @Theme
 */
public class A {
}


class Father<T,E>{
    public T show(){
        return null;
    }
}

class Son extends Father<Integer,String>{
    @Override
    public Integer show() {
        return 3;
    }
}
