package com.ryleon.app.func;

import com.ryleon.util.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @author ALiang
 * @date 2022-12-31
 * @effect 自定义分词函数
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class DwsSplitFunction extends TableFunction<Row> {
    public void eval(String str) {
        try {
            List<String> keywordList = KeywordUtil.analyze(str);
            for (String keyword : keywordList) {
                collect(Row.of(keyword));
            }
        } catch (Exception e) {
            collect(Row.of(str));
        }
    }
}