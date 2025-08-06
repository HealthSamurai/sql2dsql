package com.example.pgquery;

public class Main {
    
    public static void main(String[] args) {
        System.out.println("Starting...");

        String input = "select 1";
        
        try {
            var res = PgQuery.parse(input, "libpg_query.dylib");
            System.out.println(res);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
} 