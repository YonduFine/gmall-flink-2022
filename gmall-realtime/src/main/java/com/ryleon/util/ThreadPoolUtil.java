package com.ryleon.util;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author ALiang
 * @date 2023-01-05
 * @effect 创建线程池
 */
public class ThreadPoolUtil {

    private static ThreadPoolExecutor threadPoolExecutor;

    private ThreadPoolUtil(){

    }

    /**
     * 创建一个线程对象
     * @return ThreadPoolExecutor
     */
    public static ThreadPoolExecutor getThreadPoolExecutor(){
        if (threadPoolExecutor == null){
            synchronized (ThreadPoolUtil.class){
                if (threadPoolExecutor == null){
                    threadPoolExecutor = new ThreadPoolExecutor(4,
                        20,
                        100,
                        TimeUnit.SECONDS,
                        new LinkedBlockingDeque<>());
                }
            }
        }
        return threadPoolExecutor;
    }

}
