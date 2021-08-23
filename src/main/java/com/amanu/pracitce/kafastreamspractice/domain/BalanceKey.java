package com.amanu.pracitce.kafastreamspractice.domain;

import java.time.LocalDate;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Value;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BalanceKey {

    LocalDate date;
    int accountId;
    int subAccountId;

    public static Set<BalanceKey> generate(LocalDate date, int count){
        return IntStream.range(0, count)
                .mapToObj(i -> BalanceKey.builder()
                        .date(LocalDate.now())
                        .accountId(count + i)
                        .subAccountId(Integer.parseInt((count + i) + "" + i))
                        .build())
                .collect(Collectors.toSet());
    }
}
