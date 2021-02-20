package com.company;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

enum ExecutionMethod {
    SUBMIT,
    INVOKE_ALL,
    INVOKE_ALL_TIMEOUT
}

public class Main {
    private static long MAX_VALUE = 1000000000L;
    public static void main(String[] args) {
        Hashtable<String, ExecutionMethod> methodMap = new Hashtable<>();
        methodMap.put("submit", ExecutionMethod.SUBMIT);
        methodMap.put("invokeall", ExecutionMethod.INVOKE_ALL);
        methodMap.put("invokealltimeout", ExecutionMethod.INVOKE_ALL_TIMEOUT);
        if (args.length < 1 || !Collections.list(methodMap.keys()).contains(args[0])) {
            System.out.println("First argument should be the method of execution (\"submit\", \"invokeall\", \"invokealltimeout\")");
            return;
        }

        String executionMethodStr = args[0];
        System.out.println("Running with execution method: " + executionMethodStr);
        ExecutionMethod executionMethod = methodMap.get(executionMethodStr);
        Hashtable<ExecutionMethod, BiFunction<Stream<PrimeCallable>, ExecutorService, List<Future<PrimeCallable.PrimeResult>>>> methodInvocationMap = new Hashtable<>();
        methodInvocationMap.put(ExecutionMethod.SUBMIT, Main::withSubmit);
        methodInvocationMap.put(ExecutionMethod.INVOKE_ALL, Main::withInvokeAll);
        methodInvocationMap.put(ExecutionMethod.INVOKE_ALL_TIMEOUT, Main::withInvokeAllTimeout);

        int count = 500;
        int cores = Runtime.getRuntime()
                .availableProcessors();
        System.out.println("Number of cores: " + cores);
        var executorService =
                Executors.newFixedThreadPool(cores);
        Stream<PrimeCallable> primeStream = new Random()
                // Generate "count" random between the min and max values.
                .longs(count, MAX_VALUE - count, MAX_VALUE)
                // Convert each random number into a PrimeCallable.
                .mapToObj(PrimeCallable::new);

        // Call the corresponding method invocation type of the executor service to get back a list of futures
        List<Future<PrimeCallable.PrimeResult>> futures = methodInvocationMap.get(executionMethod).apply(primeStream, executorService);

        // While any of the futures are still executing, print them to the console.
        Callable<List<Future<PrimeCallable.PrimeResult>>> getExecuting = () -> futures.stream().filter(x -> !x.isDone()).collect(Collectors.toList());
        try {
            List<Future<PrimeCallable.PrimeResult>> executing;
            while ((executing = getExecuting.call()).size() > 0) {
                SimpleDateFormat formatter= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                var date = new Date(System.currentTimeMillis());
                System.out.println("[" + formatter.format(date) + "] There are " + executing.size() + " tasks still executing");
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        
        System.out.println("All tasks are completed.");
        executorService.shutdownNow();
    }

    public static List<Future<PrimeCallable.PrimeResult>> withSubmit(Stream<PrimeCallable> primeStream, ExecutorService executorService) {
        return primeStream
                // Submit each PrimeCallable to the ExecutorService.
                .map(executorService::submit)
                // Collect the results into a list of futures.
                .collect(Collectors.toList());
    }

    public static List<Future<PrimeCallable.PrimeResult>> withInvokeAll(Stream<PrimeCallable> primeStream, ExecutorService executorService) {
        try {
            return executorService.invokeAll(primeStream
                    .collect(Collectors.toList()));
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }

        return null;
    }

    public static List<Future<PrimeCallable.PrimeResult>> withInvokeAllTimeout(Stream<PrimeCallable> primeStream, ExecutorService executorService) {
        try {
            return executorService.invokeAll(primeStream
                    .collect(Collectors.toList()), 30L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }

        return null;
    }
}
