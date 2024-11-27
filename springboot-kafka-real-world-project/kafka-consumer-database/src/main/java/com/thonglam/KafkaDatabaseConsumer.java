package com.thonglam;


import com.thonglam.entity.WikimediaData;
import com.thonglam.repository.WikimediaDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaDatabaseConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDatabaseConsumer.class);

    WikimediaDataRepository wikimediaDataRepository;

    public KafkaDatabaseConsumer(WikimediaDataRepository wikimediaDataRepository) {
        this.wikimediaDataRepository = wikimediaDataRepository;
    }

    @KafkaListener(topics = "wikimedia_recentchange", groupId = "myGroup")
    public void consumer(String eventMessage) {
        LOGGER.info(String.format("event message received ->%s ", eventMessage));

        WikimediaData wikimediaData = new WikimediaData();
        wikimediaData.setWikimediaData(eventMessage);

        wikimediaDataRepository.save(wikimediaData);


    }
}
