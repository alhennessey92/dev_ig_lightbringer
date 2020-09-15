
import time
import sys
import traceback
import logging

from trading_ig import IGService, IGStreamService
from trading_ig.config import config
from trading_ig.lightstreamer import Subscription

from protobuf import lightbringer_pb2
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib
from uuid import uuid4

epic = "CHART:CS.D.EURUSD.MINI.IP:1MINUTE"
config_file = 'librdkafka.config'
topic = "eurusd"
conf = ccloud_lib.read_ccloud_config(config_file)


# Create topic if needed
ccloud_lib.create_topic(conf, topic)
delivered_records = 0
schema_registry_conf = {
        'url': conf['schema.registry.url'],
        'basic.auth.user.info': conf['schema.registry.basic.auth.user.info']}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
protobuf_serializer = ProtobufSerializer(lightbringer_pb2.CandlePrice,
                                            schema_registry_client)
producer_conf = {
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': protobuf_serializer}
producer = SerializingProducer(producer_conf)
print("Producing user records to topic {}. ^C to exit.".format(topic))



#///////////////////////////////////////////////
def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

# A simple function acting as a Subscription listener
def on_prices_update(item_update):
    # print("price: %s " % item_update)
    scale="1MINUTE"

    candle_ltv = ("{LTV:<5}".format(**item_update["values"]))
    # candle_ttv = ("{TTV:<5}".format(**item_update["values"]))
    candle_utm = ("{UTM:<10}".format(**item_update["values"]))
    candle_day_open_mid = ("{DAY_OPEN_MID:<5}".format(**item_update["values"]))
    candle_day_net_chg_mid = ("{DAY_NET_CHG_MID:<5}".format(**item_update["values"]))
    candle_day_perc_chg_mid = ("{DAY_PERC_CHG_MID:<5}".format(**item_update["values"]))
    candle_day_high = ("{DAY_HIGH:<5}".format(**item_update["values"]))
    candle_day_low = ("{DAY_LOW:<5}".format(**item_update["values"]))
    candle_ofr_open = ("{OFR_OPEN:<5}".format(**item_update["values"]))
    candle_ofr_high = ("{OFR_HIGH:<5}".format(**item_update["values"]))
    candle_ofr_low = ("{OFR_LOW:<5}".format(**item_update["values"]))
    candle_ofr_close = ("{OFR_CLOSE:<5}".format(**item_update["values"]))
    candle_bid_open = ("{BID_OPEN:<5}".format(**item_update["values"]))
    candle_bid_high = ("{BID_HIGH:<5}".format(**item_update["values"]))
    candle_bid_low = ("{BID_LOW:<5}".format(**item_update["values"]))
    candle_bid_close = ("{BID_CLOSE:<5}".format(**item_update["values"]))
    candle_ltp_open = ("{LTP_OPEN:<5}".format(**item_update["values"]))
    candle_ltp_high = ("{LTP_HIGH:<5}".format(**item_update["values"]))
    candle_ltp_low = ("{LTP_LOW:<5}".format(**item_update["values"]))
    candle_ltp_close = ("{LTP_CLOSE:<5}".format(**item_update["values"]))
    candle_cons_end = int("{CONS_END:<1}".format(**item_update["values"]))
    candle_cons_tick_count = int("{CONS_TICK_COUNT:<5}".format(**item_update["values"]))

    candle_ltv = int(candle_ltv)
    # candle_ttv = int(candle_ttv)

    
    candle_end = int("{CONS_END:<1}".format(**item_update["values"]))
    name = "{stock_name:<19}".format(stock_name=item_update["name"])
    
    record_key = "{UTM:<10}".format(**item_update["values"])
    new_candle_end = int(candle_end)
    
    if new_candle_end==1:

        s, ms = divmod(int(candle_utm), 1000)
        new_time = '%s.%03d' % (time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(s)), ms)
        print(new_time)

        print(
            "{stock_name:<19}: Time %s - " 
            "High {OFR_HIGH:>5} - Open {OFR_OPEN:>5} - Close {OFR_CLOSE:>5} - Low {OFR_LOW:>5} - End {CONS_END:>2}".format(
                stock_name=item_update["name"], **item_update["values"]
            ) % new_time
        )
        def acked(err, msg):
            global delivered_records
            """Delivery report handler called on
            successful or failed delivery of message
            """
            if err is not None:
                print("Failed to deliver message: {}".format(err))
            else:
                delivered_records += 1
                print("Produced record to topic {} partition [{}] @ offset {}"
                    .format(msg.topic(), msg.partition(), msg.offset()))

        
        #record_value = json.dumps({'candle_open': candle_open, 'candle_close': candle_close, 'candle_high': candle_high, 'candle_low': candle_low, 'Name': name, 'candle_end': candle_end})
        #print("Producing record: {}\t{}".format(record_key))
        uid = str(uuid4())
        candle = lightbringer_pb2.CandlePrice(uuid=uid, 
                                        epic=epic, 
                                        scale=scale, 
                                        ltv=candle_ltv, 
                                        # ttv=candle_ttv, 
                                        day_open_mid=candle_day_open_mid,
                                        day_net_chg_mid=candle_day_net_chg_mid,
                                        day_perc_chg_mid=candle_day_perc_chg_mid,
                                        day_high=candle_day_high,
                                        day_low=candle_day_low,
                                        ofr_open=candle_ofr_open,
                                        ofr_high=candle_ofr_high,
                                        ofr_low=candle_ofr_low,
                                        ofr_close=candle_ofr_close,
                                        bid_open=candle_bid_open,
                                        bid_high=candle_bid_high,
                                        bid_low=candle_bid_low,
                                        bid_close=candle_bid_close,
                                        ltp_open=candle_ltp_open,
                                        ltp_high=candle_ltp_high,
                                        ltp_low=candle_ltp_low,
                                        ltp_close=candle_ltp_close,
                                        cons_end=candle_cons_end,
                                        cons_tick_count=candle_cons_tick_count)
        producer.produce(topic=topic, key=uid, value=candle, on_delivery=delivery_report)
        # producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
        producer.poll(0)

    


def on_heartbeat_update(item_update):
    print(item_update["values"])

def on_account_update(balance_update):
    print("balance: %s " % balance_update)


def main():
    logging.basicConfig(level=logging.INFO)
    # logging.basicConfig(level=logging.DEBUG)

    ig_service = IGService(
        config.username, config.password, config.api_key, config.acc_type
    )

    ig_stream_service = IGStreamService(ig_service)
    ig_session = ig_stream_service.create_session()
    # Ensure configured account is selected
    accounts = ig_session[u"accounts"]
    for account in accounts:
        if account[u"accountId"] == config.acc_number:
            accountId = account[u"accountId"]
            break
        else:
            print("Account not found: {0}".format(config.acc_number))
            accountId = None
    ig_stream_service.connect(accountId)

    # Making a new Subscription in MERGE mode
    subscription_prices = Subscription(
        mode="MERGE",
        items=["CHART:CS.D.EURUSD.MINI.IP:1MINUTE"],
        fields=["LTV", "UTM", "DAY_OPEN_MID", "DAY_NET_CHG_MID", "DAY_PERC_CHG_MID", "DAY_HIGH", "DAY_LOW", "OFR_OPEN", "OFR_HIGH", "OFR_LOW", "OFR_CLOSE", "BID_OPEN", "BID_HIGH", "BID_LOW", "BID_CLOSE", "LTP_OPEN", "LTP_HIGH", "LTP_LOW", "LTP_CLOSE", "CONS_END", "CONS_TICK_COUNT"],
    )
    # adapter="QUOTE_ADAPTER")

    # Adding the "on_price_update" function to Subscription
    subscription_prices.addlistener(on_prices_update)

    # Registering the Subscription
    sub_key_prices = ig_stream_service.ls_client.subscribe(subscription_prices)

    # Making an other Subscription in MERGE mode
    subscription_account = Subscription(
        mode="MERGE", items=["ACCOUNT:" + accountId], fields=["AVAILABLE_CASH"],
    )
    #    #adapter="QUOTE_ADAPTER")

    # Adding the "on_balance_update" function to Subscription
    subscription_account.addlistener(on_account_update)

    # Registering the Subscription
    sub_key_account = ig_stream_service.ls_client.subscribe(subscription_account)






    heartbeat_items = ["TRADE:HB.U.HEARTBEAT.IP"]
    heartbeat = Subscription(
        mode='MERGE',
        items=heartbeat_items,
        fields=["HEARTBEAT"],
    )

    heartbeat.addlistener(on_heartbeat_update)
    sub_heartbeat = ig_stream_service.ls_client.subscribe(heartbeat)




    input(
        "{0:-^80}\n".format(
            "HIT CR TO UNSUBSCRIBE AND DISCONNECT FROM \
    LIGHTSTREAMER"
        )
    )

    # Disconnecting
    ig_stream_service.disconnect()
    producer.flush()


if __name__ == "__main__":
    main()