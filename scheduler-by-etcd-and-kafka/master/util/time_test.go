package util

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"go-tools/scheduler-by-kafka/pb"
)

func TestGetNextScheduleTimestamp(t *testing.T) {
	now := time.Now()
	ap := &pb.AlarmPolicyProto{
		Schedule: &pb.AlarmScheduleProto{
			IntervalMinute: 10,
		},
		NextScheduleTimestamp: now.UnixMilli(),
	}
	res := GetNextScheduleTimestamp(ap)
	assert.Equal(t, now.UnixMilli()+10*time.Minute.Milliseconds(), res)

	ap = &pb.AlarmPolicyProto{
		Schedule: &pb.AlarmScheduleProto{
			IntervalMinute: 0,
			FixTimeMinute:  10,
			DayOfWeek:      0,
		},
		NextScheduleTimestamp: now.UnixMilli(),
	}
	res = GetNextScheduleTimestamp(ap)
	zeroTime := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	expectTime := zeroTime.UnixMilli() + 10*time.Minute.Milliseconds() + 24*time.Hour.Milliseconds()
	assert.Equal(t, expectTime, res)

	ap = &pb.AlarmPolicyProto{
		Schedule: &pb.AlarmScheduleProto{
			IntervalMinute: 0,
			FixTimeMinute:  23*60 + 59,
			DayOfWeek:      7,
		},
		NextScheduleTimestamp: now.UnixMilli(),
	}
	res = GetNextScheduleTimestamp(ap)
	expectTime = zeroTime.UnixMilli() + int64(24*(6-int(now.Weekday()+6)%7)+23)*time.Hour.Milliseconds() + 59*time.Minute.Milliseconds()
	assert.Equal(t, expectTime, res)

	dayOfWeek := now.Weekday() - 1
	if dayOfWeek <= 0 {
		dayOfWeek += 7
	}
	ap = &pb.AlarmPolicyProto{
		Schedule: &pb.AlarmScheduleProto{
			IntervalMinute: 0,
			FixTimeMinute:  10,
			DayOfWeek:      int32(dayOfWeek),
		},
		NextScheduleTimestamp: now.UnixMilli(),
	}
	res = GetNextScheduleTimestamp(ap)
	expectTime = zeroTime.UnixMilli() + int64(6*24*60+10)*time.Minute.Milliseconds()
	assert.Equal(t, expectTime, res)
}
