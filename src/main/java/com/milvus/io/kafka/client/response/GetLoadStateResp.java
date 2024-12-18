package com.milvus.io.kafka.client.response;

import lombok.Data;

@Data
public class GetLoadStateResp {
    private String loadState;
    private String loadProgress;
    private String message;
}
