package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Balance;
import com.jpmc.midascore.repository.UserRecordRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


@RestController
public class BalanceController {

    @Autowired
    private UserRecordRepository UserRecordRepository;

    @GetMapping("/balance")
    public Balance getBalance(@RequestParam("userId") Long userId) {
        UserRecord userRecord = UserRecordRepository.findById(userId);
        
        if (userRecord != null) {
            return new Balance(userRecord.getBalance());
        } else {
            return new Balance(0.0f);
        }
    }
}
