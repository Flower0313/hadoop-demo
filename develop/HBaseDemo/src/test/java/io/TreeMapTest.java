package io;


import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiConsumer;

/**
 * @ClassName MapReduceDemo-TreeMapTest
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年10月10日20:28 - 周日
 * @Describe
 */
public class TreeMapTest {

    public static void main(String[] args) {
        //TreeMap<>(排序器)
        Map<Student, Integer> map = new TreeMap<>(new Comparator<Student>() {
            public int compare(Student p1, Student p2) {
                if (p1.score == p2.score) {
                    return 0;
                }
                return p1.score > p2.score ? -1 : 1;
            }
        });
        map.put(new Student("Tom", 77), 1);
        map.put(new Student("Bob", 66), 2);
        map.put(new Student("Lily", 99), 3);
        for (Student key : map.keySet()) {
            System.out.println(key);
        }
        System.out.println(map.get(new Student("Bob", 66))); // 2
    }
}

class Student {
    public String name;
    public int score;

    Student(String name, int score) {
        this.name = name;
        this.score = score;
    }

    public String toString() {
        return String.format("{%s: score=%d}", name, score);
    }
}
