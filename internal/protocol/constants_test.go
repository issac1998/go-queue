package protocol

import (
	"testing"
)

func TestRequestTypeNames(t *testing.T) {
	tests := []struct {
		requestType int32
		expected    string
	}{
		{ProduceRequestType, "PRODUCE"},
		{FetchRequestType, "FETCH"},
		{CreateTopicRequestType, "CREATE_TOPIC"},
		{JoinGroupRequestType, "JOIN_GROUP"},
		{LeaveGroupRequestType, "LEAVE_GROUP"},
		{HeartbeatRequestType, "HEARTBEAT"},
		{CommitOffsetRequestType, "COMMIT_OFFSET"},
		{FetchOffsetRequestType, "FETCH_OFFSET"},
		{ListGroupsRequestType, "LIST_GROUPS"},
		{DescribeGroupRequestType, "DESCRIBE_GROUP"},
		{ListTopicsRequestType, "LIST_TOPICS"},
		{DeleteTopicRequestType, "DELETE_TOPIC"},
		{GetTopicInfoRequestType, "GET_TOPIC_INFO"},
		{999, "UNKNOWN"},
	}

	for _, test := range tests {
		result := GetRequestTypeName(test.requestType)
		if result != test.expected {
			t.Errorf("GetRequestTypeName(%d) = %s, expected %s", test.requestType, result, test.expected)
		}
	}
}

func TestErrorCodeNames(t *testing.T) {
	tests := []struct {
		errorCode int16
		expected  string
	}{
		{ErrorNone, "NONE"},
		{ErrorInvalidRequest, "INVALID_REQUEST"},
		{ErrorInvalidTopic, "INVALID_TOPIC"},
		{ErrorUnknownPartition, "UNKNOWN_PARTITION"},
		{ErrorInvalidMessage, "INVALID_MESSAGE"},
		{ErrorMessageTooLarge, "MESSAGE_TOO_LARGE"},
		{ErrorOffsetOutOfRange, "OFFSET_OUT_OF_RANGE"},
		{ErrorBrokerNotAvailable, "BROKER_NOT_AVAILABLE"},
		{ErrorFetchFailed, "FETCH_FAILED"},
		{ErrorProduceFailed, "PRODUCE_FAILED"},
		{ErrorUnauthorized, "UNAUTHORIZED"},
		{ErrorQuotaExceeded, "QUOTA_EXCEEDED"},
		{999, "UNKNOWN_ERROR"},
	}

	for _, test := range tests {
		result := GetErrorCodeName(test.errorCode)
		if result != test.expected {
			t.Errorf("GetErrorCodeName(%d) = %s, expected %s", test.errorCode, result, test.expected)
		}
	}
}

func TestConstants(t *testing.T) {

	if ProtocolVersion != 1 {
		t.Errorf("ProtocolVersion = %d, expected 1", ProtocolVersion)
	}

	expectedRequestTypes := map[string]int32{
		"PRODUCE":        1,
		"FETCH":          2,
		"LIST_TOPICS":    3,
		"CREATE_TOPIC":   4,
		"DELETE_TOPIC":   5,
		"JOIN_GROUP":     10,
		"LEAVE_GROUP":    11,
		"HEARTBEAT":      12,
		"COMMIT_OFFSET":  13,
		"FETCH_OFFSET":   14,
		"GET_TOPIC_INFO": 15,
		"LIST_GROUPS":    30,
		"DESCRIBE_GROUP": 31,
	}

	for name, expectedValue := range expectedRequestTypes {
		for requestType, actualName := range RequestTypeNames {
			if actualName == name && requestType != expectedValue {
				t.Errorf("Request type %s has value %d, expected %d", name, requestType, expectedValue)
			}
		}
	}

	expectedErrorCodes := map[string]int16{
		"NONE":                 0,
		"INVALID_REQUEST":      1,
		"INVALID_TOPIC":        2,
		"UNKNOWN_PARTITION":    3,
		"INVALID_MESSAGE":      4,
		"TOPIC_ALREADY_EXISTS": 5,
		"TOPIC_NOT_FOUND":      6,
		"PARTITION_NOT_FOUND":  7,
		"INVALID_OFFSET":       8,
		"GROUP_NOT_FOUND":      9,
		"BROKER_NOT_AVAILABLE": 10,
		"FETCH_FAILED":         11,
		"PRODUCE_FAILED":       12,
		"UNAUTHORIZED":         20,
		"QUOTA_EXCEEDED":       21,
	}

	for name, expectedValue := range expectedErrorCodes {
		for errorCode, actualName := range ErrorCodeNames {
			if actualName == name && errorCode != expectedValue {
				t.Errorf("Error code %s has value %d, expected %d", name, errorCode, expectedValue)
			}
		}
	}
}

func TestRequestTypeMapping(t *testing.T) {

	allRequestTypes := []int32{
		ProduceRequestType,
		FetchRequestType,
		CreateTopicRequestType,
		JoinGroupRequestType,
		LeaveGroupRequestType,
		HeartbeatRequestType,
		CommitOffsetRequestType,
		FetchOffsetRequestType,
		ListGroupsRequestType,
		DescribeGroupRequestType,
		ListTopicsRequestType,
		DeleteTopicRequestType,
		GetTopicInfoRequestType,
	}

	for _, requestType := range allRequestTypes {
		name := GetRequestTypeName(requestType)
		if name == "UNKNOWN" {
			t.Errorf("Request type %d has no name mapping", requestType)
		}
	}
}

func TestErrorCodeMapping(t *testing.T) {

	allErrorCodes := []int16{
		ErrorNone,
		ErrorInvalidRequest,
		ErrorInvalidTopic,
		ErrorUnknownPartition,
		ErrorInvalidMessage,
		ErrorMessageTooLarge,
		ErrorOffsetOutOfRange,
		ErrorBrokerNotAvailable,
		ErrorFetchFailed,
		ErrorProduceFailed,
		ErrorUnauthorized,
		ErrorQuotaExceeded,
	}

	for _, errorCode := range allErrorCodes {
		name := GetErrorCodeName(errorCode)
		if name == "UNKNOWN_ERROR" {
			t.Errorf("Error code %d has no name mapping", errorCode)
		}
	}
}
