package com.ryleon.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author ALiang
 * @date 2022-12-31
 * @effect IK分词工具类
 */
public class KeywordUtil {
    public static List<String> analyze(String text) throws Exception {
        ArrayList<String> keywordList = new ArrayList<>();
        StringReader stringReader = new StringReader(text);
        IKSegmenter ikSegmenter = new IKSegmenter(stringReader, false);
        Lexeme next = ikSegmenter.next();
        while (next != null) {
            String keyword = next.getLexemeText();
            keywordList.add(keyword);
            next = ikSegmenter.next();
        }

        return keywordList;
    }

    public static void main(String[] args) throws Exception {
        System.out.println(analyze("河南工程学院大数据技术2121"));
    }
}
