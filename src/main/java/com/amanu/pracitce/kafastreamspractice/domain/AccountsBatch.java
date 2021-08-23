package com.amanu.pracitce.kafastreamspractice.domain;

import java.io.Serializable;
import java.util.Set;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor(staticName = "of")
@NoArgsConstructor
public class AccountsBatch implements Serializable {
    Set<BalanceKey> accounts;
}
