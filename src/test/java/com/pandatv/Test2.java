package com.pandatv;

import org.junit.Test;

import java.util.Objects;

/**
 * @author: likaiqing
 * @create: 2018-12-19 10:41
 **/
public class Test2 {
    @Test
    public void test1() {
        int a = 0;
        changeNum(a);
        System.out.println(a);
        Person person = new Person("kaiqing", 28);
        System.out.println(person);
        changePerson(person);
        System.out.println(person);
        String str = new String("likaiqing");
        System.out.println(str);
        String str1 = changeStr(str);
        System.out.println(str);
        System.out.println(str==str1);
        System.out.println(str.equals(str1));
    }

    private String changeStr(String str) {
        str = new String("likaiqing");
        return str;
    }

    private void changePerson(Person person) {
//        person = new Person("kaiqing1", 28);
        person.setAge(29);
    }

    public void changeNum(int a) {
        a = 2;
    }

    class Person {
        private String name;
        private int age;


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Person person = (Person) o;
            return age == person.age &&
                    Objects.equals(name, person.name);
        }

        @Override
        public int hashCode() {

            return Objects.hash(name, age);
        }

        public Person() {
        }

        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }

}
