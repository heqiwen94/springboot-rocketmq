package com.hqw.websocket.rocketmq.comsumer.pull;

import com.hqw.websocket.rocketmq.handle.GatherControlVo;
import com.hqw.websocket.rocketmq.handle.MQHandleImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.*;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.nutz.dao.Dao;
import org.nutz.json.Json;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 拉模式
 */
@Component
@Slf4j
public class PullConsumer {

    @Value("${rocketmq.consumer.namesrvAddr}")
    private String namesrvAddr;
    @Value("${rocketmq.consumer.groupName}")
    private String groupName;
    @Value("${rocketmq.mqTopic}")
    private String mqTopic;
    @Value("${rocketmq.mqTag}")
    private String mqTag;

    @Autowired
    private MQHandleImpl mqHandle;

    @Autowired
    private Dao nutzDao;


    @Bean
    public void getRocketMQConsumer() throws Exception {
        log.info("开始消费消息");
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(groupName);
        consumer.setNamesrvAddr(namesrvAddr);

        MQPullConsumerScheduleService scheduleService = new MQPullConsumerScheduleService(groupName);
        scheduleService.setPullThreadNums(1);
        scheduleService.setDefaultMQPullConsumer(consumer);
        scheduleService.registerPullTaskCallback(mqTopic, new PullTaskCallback() {

            @Override
            public void doPullTask(MessageQueue mq, PullTaskContext context) {
                MQPullConsumer pullConsumer = context.getPullConsumer();
                try {
                    long offset = pullConsumer.fetchConsumeOffset(mq, false);
                    PullResult pull = pullConsumer.pull(mq, "*", offset, 32);
                    switch (pull.getPullStatus()) {
                        case FOUND:
                            // 结果输出
                            handle(pull);
                            break;
                        case NO_MATCHED_MSG:
                            break;
                        default:
                    }
                    // 获取下一个循环的offset
                    pullConsumer.updateConsumeOffset(mq, pull.getNextBeginOffset());
                    // 设置下次访问时间
                    context.setPullNextDelayTimeMillis(1000);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        scheduleService.start();
    }

    private void handle(PullResult pullResult) {
        List<MessageExt> messageExtList = pullResult.getMsgFoundList();
        log.info("消费者接收到的数量：" + messageExtList.size());
        for (MessageExt messageExt : messageExtList) {
            long beginTime = System.currentTimeMillis();
            String msg = new String(messageExt.getBody());
//            log.info("接收到的消息是：" + msg);
            if (messageExt.getTags().equals(mqTag)) {
                // 处理对应的业务逻辑
                GatherControlVo gatherControlVo = Json.fromJson(GatherControlVo.class, msg);
//                log.info("gatherControlVo:{}", gatherControlVo);
                try {
                    // 根据类型保存到数据库; 并更新采集指令状态
                    mqHandle.handle(gatherControlVo);
                } catch (Throwable e) {
                    log.error("rocketMQ保存数据入库失败:" + e);
                    //  记录messageId,等待问题排查
//                    this.saveRocketMqLog(messageExt, e);
                }
                log.info("插入一条MQ数据 执行时间:" + (System.currentTimeMillis() - beginTime) + "ms" +
                        ";上传的数据:" + gatherControlVo.getData().size() +
                        ";UUID:" + gatherControlVo.getTransId() +
                        ";医院orgId:" + gatherControlVo.getOrgId() +
                        ";数据类型:" + gatherControlVo.getDataType());
            }
        }
    }


    /**
     * 保存消费rocketMQ的异常信息
     *
     * @param messageExt
     * @param e
     */
//    private void saveRocketMqLog(MessageExt messageExt, Throwable e) {
//        String messageId = messageExt.getMsgId();
//        String msg = new String(messageExt.getBody());
//        GatherRocketMqLogEntity entity = nutzDao.fetch(GatherRocketMqLogEntity.class, Cnd.where("message_id", "=", messageId));
//        if (null == entity) {
//            entity = new GatherRocketMqLogEntity();
//            entity.setMessageId(messageId);
//            entity.setTopic(messageExt.getTopic());
//            entity.setTags(messageExt.getTags());
//            entity.setContent(msg);
//            if (null == e) {
//                entity.setErrMsg(null);
//            } else {
//                entity.setErrMsg(e.getMessage());
//            }
//            entity.setCreatedDate(new Date());
//            entity.setUpdatedDate(new Date());
//            nutzDao.fastInsert(entity);
//        } else {
//            entity.setUpdatedDate(new Date());
//            entity.setContent(msg);
//            if (null == e) {
//                entity.setErrMsg(null);
//            } else {
//                entity.setErrMsg(e.getMessage());
//            }
//            nutzDao.update(entity, Cnd.where("message_id", "=", messageId));
//        }
//    }


}
