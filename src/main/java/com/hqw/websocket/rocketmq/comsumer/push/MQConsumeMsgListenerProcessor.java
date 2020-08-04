package com.hqw.websocket.rocketmq.comsumer.push;//package com.boyi.him.rocketmq.comsumer;

import com.hqw.websocket.rocketmq.handle.GatherControlVo;
import com.hqw.websocket.rocketmq.handle.MQHandleImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.nutz.dao.Dao;
import org.nutz.json.Json;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.util.List;

/**
 * 推模式：当消费者性能跟不上broker推送的效率，就回产生消息延迟
 */
@Component
@Slf4j
public class MQConsumeMsgListenerProcessor implements MessageListenerConcurrently {

    @Resource
    private MQHandleImpl mqHandle;

    @Value("${rocketmq.mqTopic}")
    private String mqTopic;

    @Value("${rocketmq.mqTag}")
    private String mqTag;

    @Autowired
    private Dao nutzDao;


    /**
     * 默认msgs里只有一条消息，可以通过设置consumeMessageBatchMaxSize参数来批量接收消息<br/>
     * 不要抛异常，如果没有return CONSUME_SUCCESS ，consumer会重新消费该消息，直到return CONSUME_SUCCESS
     */
    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        if (CollectionUtils.isEmpty(msgs)) {
            log.info("接收到的消息为空，不做任何处理");
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }
        MessageExt messageExt = msgs.get(0);

        String msg = new String(messageExt.getBody());
        log.info("接收到的消息是：" + msg);
        if (messageExt.getTopic().equals(mqTopic)) {
            if (messageExt.getTags().equals(mqTag)) {
                // 判断该消息是否重复消费（RocketMQ不保证消息不重复，如果你的业务需要保证严格的不重复消息，需要你自己在业务端去重）
                // 获取该消息重试次数
                int reconsumeTimes = messageExt.getReconsumeTimes();
                if (reconsumeTimes == 3) { //消息已经重试了3次，如果不需要再次消费，则返回成功
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
                // 处理对应的业务逻辑
                GatherControlVo gatherControlVo = Json.fromJson(GatherControlVo.class, msg);
                log.info("gatherControlVo:{}", gatherControlVo);
                // 根据类型保存到数据库; 并更新采集指令状态
                try {
                    mqHandle.handle(gatherControlVo);
                } catch (Throwable e) {
                    log.error("rocketMQ保存数据入库失败:" + e);
//                    throw new RuntimeException("保存数据入库失败:" + e.getMessage());
                    //  记录messageId,等待问题排查
                    this.saveRocketMqLog(messageExt, e);
                }
            }
        }
        // 如果没有return success ，consumer会重新消费该消息，直到return success
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    /**
     * 保存消费rocketMQ的异常信息
     *
     * @param messageExt
     * @param e
     */
    private void saveRocketMqLog(MessageExt messageExt, Throwable e) {
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
    }


}
