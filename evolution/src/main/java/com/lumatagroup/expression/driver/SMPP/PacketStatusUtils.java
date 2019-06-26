package com.lumatagroup.expression.driver.SMPP;

import ie.omk.smpp.message.SMPPPacket;
import ie.omk.smpp.util.PacketStatus;

public class PacketStatusUtils {

	public static String getStatus(int messageStatus) {
		switch (messageStatus) {
			case SMPPPacket.SM_STATE_DELIVERED: return "DELIVERED";
			case SMPPPacket.SM_STATE_ACCEPTED: return "DELIVERED";
			case SMPPPacket.SM_STATE_EXPIRED: return "EXPIRED";
			case SMPPPacket.SM_STATE_DELETED: return "EXPIRED";
			case SMPPPacket.SM_STATE_UNDELIVERABLE: return "UNDELIVERABLE";
			case SMPPPacket.SM_STATE_EN_ROUTE: return "PENDING";
			case SMPPPacket.SM_STATE_INVALID: return "INVALID";
			case 8: return "REJECTED";
			default: return "UNKNOWN";
		}
	}
	
	public static String getMessage(int commandStatus) {
		switch(commandStatus) {
		case PacketStatus.OK: return "OK";
	    case PacketStatus.INVALID_MESSAGE_LEN: return "INVALID_MESSAGE_LEN";
	    case PacketStatus.INVALID_COMMAND_LEN: return "INVALID_COMMAND_LEN";
	    case PacketStatus.INVALID_COMMAND_ID: return "INVALID_COMMAND_ID";
	    case PacketStatus.INVALID_BIND_STATUS: return "INVALID_BIND_STATUS";
	    case PacketStatus.ALREADY_BOUND: return "ALREADY_BOUND";
	    case PacketStatus.INVALID_PRIORITY_FLAG: return "INVALID_PRIORITY_FLAG";
	    case PacketStatus.INVALID_REGISTERED_DELIVERY_FLAG: return "INVALID_REGISTERED_DELIVERY_FLAG";
	    case PacketStatus.SYSTEM_ERROR: return "SYSTEM_ERROR";
	    case PacketStatus.INVALID_SOURCE_ADDRESS: return "INVALID_SOURCE_ADDRESS";
	    case PacketStatus.INVALID_DEST_ADDRESS: return "INVALID_DEST_ADDRESS";
	    case PacketStatus.INVALID_MESSAGE_ID: return "INVALID_MESSAGE_ID";
	    case PacketStatus.BIND_FAILED: return "BIND_FAILED";
	    case PacketStatus.INVALID_PASSWORD: return "INVALID_PASSWORD";
	    case PacketStatus.INVALID_SYSTEM_ID: return "INVALID_SYSTEM_ID";
	    case PacketStatus.CANCEL_SM_FAILED: return "CANCEL_SM_FAILED";
	    case PacketStatus.REPLACE_SM_FAILED: return "REPLACE_SM_FAILED";
	    case PacketStatus.MESSAGE_QUEUE_FULL: return "MESSAGE_QUEUE_FULL";
	    case PacketStatus.INVALID_SERVICE_TYPE: return "INVALID_SERVICE_TYPE";
	    case PacketStatus.INVALID_NUMBER_OF_DESTINATIONS: return "INVALID_NUMBER_OF_DESTINATIONS";
	    case PacketStatus.INVALID_DISTRIBUTION_LIST: return "INVALID_DISTRIBUTION_LIST";
	    case PacketStatus.INVALID_DESTINATION_FLAG: return "INVALID_DESTINATION_FLAG";
	    case PacketStatus.INVALID_SUBMIT_WITH_REPLACE: return "INVALID_SUBMIT_WITH_REPLACE";
	    case PacketStatus.INVALID_ESM_CLASS: return "INVALID_ESM_CLASS";
	    case PacketStatus.SUBMIT_TO_DISTRIBUTION_LIST_FAILED: return "SUBMIT_TO_DISTRIBUTION_LIST_FAILED";
	    case PacketStatus.SUBMIT_FAILED: return "SUBMIT_FAILED";
	    case PacketStatus.INVALID_SOURCE_TON: return "INVALID_SOURCE_TON";
	    case PacketStatus.INVALID_SOURCE_NPI: return "INVALID_SOURCE_NPI";
	    case PacketStatus.INVALID_DESTINATION_TON: return "INVALID_DESTINATION_TON";
	    case PacketStatus.INVALID_DESTINATION_NPI: return "INVALID_DESTINATION_NPI";
	    case PacketStatus.INVALID_SYSTEM_TYPE: return "INVALID_SYSTEM_TYPE";
	    case PacketStatus.INVALID_REPLACE_IF_PRESENT_FLAG: return "INVALID_REPLACE_IF_PRESENT_FLAG";
	    case PacketStatus.INVALID_NUMBER_OF_MESSAGES: return "INVALID_NUMBER_OF_MESSAGES";
	    case PacketStatus.THROTTLING_ERROR: return "THROTTLING_ERROR";
	    case PacketStatus.INVALID_SCHEDULED_DELIVERY_TIME: return "INVALID_SCHEDULED_DELIVERY_TIME";
	    case PacketStatus.INVALID_EXPIRY_TIME: return "INVALID_EXPIRY_TIME";
	    case PacketStatus.INVALID_PREDEFINED_MESSAGE: return "INVALID_PREDEFINED_MESSAGE";
	    case PacketStatus.RECEIVER_TEMPORARY_ERROR: return "RECEIVER_TEMPORARY_ERROR";
	    case PacketStatus.RECEIVER_PERMANENT_ERROR: return "RECEIVER_PERMANENT_ERROR";
	    case PacketStatus.RECEIVER_REJECT_MESSAGE: return "RECEIVER_REJECT_MESSAGE";
	    case PacketStatus.QUERY_SM_FAILED: return "QUERY_SM_FAILED";
	    case PacketStatus.INVALID_OPTIONAL_PARAMETERS: return "INVALID_OPTIONAL_PARAMETERS";
	    case PacketStatus.OPTIONAL_PARAMETER_NOT_ALLOWED: return "OPTIONAL_PARAMETER_NOT_ALLOWED";
	    case PacketStatus.INVALID_PARAMETER_LENGTH: return "INVALID_PARAMETER_LENGTH";
	    case PacketStatus.MISSING_EXPECTED_PARAMETER: return "MISSING_EXPECTED_PARAMETER";
	    case PacketStatus.INVALID_PARAMETER_VALUE: return "INVALID_PARAMETER_VALUE";
	    case PacketStatus.DELIVERY_FAILED: return "DELIVERY_FAILED";
	    default: return null;
		}
	}
}
