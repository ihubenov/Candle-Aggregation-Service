package com.multibank.application.config;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

import java.sql.Timestamp;

@Converter(autoApply = true)
public class TimestampConverter implements AttributeConverter<Long, Timestamp> {

    @Override
    public java.sql.Timestamp convertToDatabaseColumn(Long attribute) {
        if (attribute == null) {
            return null;
        }
        // Convert epoch milliseconds (long) to Timestamp
        return new java.sql.Timestamp(attribute);
    }

    @Override
    public Long convertToEntityAttribute(java.sql.Timestamp dbData) {
        if (dbData == null) {
            return null;
        }
        // Convert Timestamp to epoch milliseconds (long)
        return dbData.getTime();
    }
}
