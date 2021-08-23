package com.amanu.pracitce.kafastreamspractice.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DailyPosition {

    int totalBalance;

    public DailyPosition plus(int balance) {
        return new DailyPosition(this.totalBalance + balance);
    }
}
