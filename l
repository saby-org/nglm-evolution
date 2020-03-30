        "deliveryType"              : "purchaseFulfillment",
        "requestClass"              : "com.evolving.nglm.evolution.PurchaseFulfillmentManager$PurchaseFulfillmentRequest",
        "requestTopic"              : "${topic.fulfillment.purchasefulfillment.request}",
        "responseTopic"             : "${topic.fulfillment.purchasefulfillment.response}",
        "internalTopic"             : "${topic.fulfillment.purchasefulfillment.internal}",
        "routingTopic"              : "${topic.fulfillment.purchasefulfillment.routing}",
        "deliveryRatePerMinute"     : 60,
        "deliveryGuarantee"         : "atLeastOnce"
      },
      {
        "deliveryType"              : "pointFulfillment",
        "requestClass"              : "com.evolving.nglm.evolution.PointFulfillmentRequest",
        "requestTopic"              : "${topic.pointfulfillment.request}",
        "responseTopic"             : "${topic.pointfulfillment.response}",
        "providerID"                : "provider_Point",
        "providerName"              : "Points provider"
      },
      {
        "deliveryType"                     : "rewardManager",
        "requestClass"                     : "com.evolving.nglm.evolution.RewardManagerRequest",
        "requestTopic"                     : "${topic.fulfillment.rewardmanager.request}",
        "responseTopic"                    : "${topic.fulfillment.rewardmanager.response}",
        "internalTopic"                    : "${topic.fulfillment.rewardmanager.internal}",
        "routingTopic"                     : "${topic.fulfillment.rewardmanager.routing}",
        "deliveryRatePerMinute"            : 60,
        "deliveryGuarantee"                : "atLeastOnce",
        "providerID"                       : "5",
        "providerName"                     : "RewardManager",
        "profileExternalSubscriberIDField" : "subscriber.msisdn"
      },
      {
        "deliveryType"              : "journeyFulfillment",
        "requestClass"              : "com.evolving.nglm.evolution.JourneyRequest",
        "requestTopic"              : "${topic.journeyrequest}",
        "responseTopic"             : "${topic.journeyresponse}",
        "providerID"                : "provider_Journey",
        "providerName"              : "Journeys provider"
      },
      {
        "deliveryType"              : "loyaltyProgramFulfillment",
        "requestClass"              : "com.evolving.nglm.evolution.LoyaltyProgramRequest",
        "requestTopic"              : "${topic.loyaltyprogramrequest}",
        "responseTopic"             : "${topic.loyaltyprogramresponse}",
        "providerID"                : "provider_Loyalty_Program",
        "providerName"              : "Loyalty Programs provider"
      },
      {
        "deliveryType"              : "emptyFulfillment_Evolution",
        "requestClass"              : "com.evolving.nglm.evolution.EmptyFulfillmentManager$EmptyFulfillmentRequest",
        "requestTopic"              : "${topic.emptyfulfillment.1.request}",
        "responseTopic"             : "${topic.emptyfulfillment.1.response}",
        "internalTopic"             : "${topic.emptyfulfillment.1.internal}",
        "routingTopic"              : "${topic.emptyfulfillment.1.routing}",
        "deliveryRatePerMinute"     : 60,
        "deliveryGuarantee"         : "atLeastOnce",
        "providerID"                : "1",
        "providerName"              : "Evolution"
      },
      {
        "deliveryType"              : "emptyFulfillment_PostpaidBilling",
        "requestClass"              : "com.evolving.nglm.evolution.EmptyFulfillmentManager$EmptyFulfillmentRequest",
        "requestTopic"              : "${topic.emptyfulfillment.3.request}",
        "responseTopic"             : "${topic.emptyfulfillment.3.response}",
        "internalTopic"             : "${topic.emptyfulfillment.3.internal}",
        "routingTopic"              : "${topic.emptyfulfillment.3.routing}",
        "deliveryRatePerMinute"     : 60,
        "deliveryGuarantee"         : "atLeastOnce",
        "providerID"                : "3",
        "providerName"              : "PostpaidBilling"
      },
      {
        "deliveryType"              : "emptyFulfillment_Store",
        "requestClass"              : "com.evolving.nglm.evolution.EmptyFulfillmentManager$EmptyFulfillmentRequest",
        "requestTopic"              : "${topic.emptyfulfillment.4.request}",
        "responseTopic"             : "${topic.emptyfulfillment.4.response}",
        "internalTopic"             : "${topic.emptyfulfillment.4.internal}",
        "routingTopic"              : "${topic.emptyfulfillment.4.routing}",
        "deliveryRatePerMinute"     : 60,
        "deliveryGuarantee"         : "atLeastOnce",
        "providerID"                : "4",
        "providerName"              : "Store"
      },
      {
        "deliveryType"              : "commodityDelivery",
        "requestClass"              : "com.evolving.nglm.evolution.CommodityDeliveryManager$CommodityDeliveryRequest",
        "requestTopic"              : "${topic.commoditydelivery.request}",
        "responseTopic"             : "${topic.commoditydelivery.response}",
        "internalTopic"             : "${topic.commoditydelivery.internal}",
        "routingTopic"              : "${topic.commoditydelivery.routing}",
        "deliveryRatePerMinute"     : 60,
        "deliveryGuarantee"         : "atLeastOnce"
      },
      {
