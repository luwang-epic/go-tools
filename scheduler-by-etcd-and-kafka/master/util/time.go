package util

import (
	"time"

	"go-tools/scheduler-by-etcd-and-kafka/pb"
)

func TimeFormat(tm int64) string {
	t := time.UnixMilli(tm)
	t = t.In(time.FixedZone("", +8*3600))
	return t.Format("2006-01-02 15:04:05")
}

func TimeUTCFormat(tm int64) string {
	return time.UnixMilli(tm).UTC().Format("2006-01-02T15:04:05Z")
}

func GetNextScheduleTimestamp(policy *pb.AlarmPolicyProto) int64 {
	next := policy.GetNextScheduleTimestamp() + policy.GetSchedule().GetIntervalMinute()*time.Minute.Milliseconds()

	// 获取当前时间
	now := time.Now()
	if policy.GetSchedule().IntervalMinute == 0 {
		dayOfWeek := int(policy.GetSchedule().GetDayOfWeek())
		zeroTime := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
		nowMilli := now.UnixMilli()
		nextMilli := zeroTime.UnixMilli() + int64(policy.GetSchedule().GetFixTimeMinute())*time.Minute.Milliseconds()
		if dayOfWeek == 0 {
			// 现在时间没有超过设置的时间，下一次调度为设置的时间
			if nowMilli <= nextMilli {
				next = nextMilli
			} else {
				// 超过一天了，加上24小时, 取下一天的定时时间
				next = nextMilli + 24*time.Hour.Milliseconds()
			}
		} else {
			// 按周调度，计算距离下一次调度的天数
			days := dayOfWeek - int(now.Weekday()+6)%7 - 1
			if days == 0 {
				// 当前时间超过了设置的时间，下一次调度为7天后
				if nowMilli > nextMilli {
					days = 7
				}
			} else if days < 0 {
				days += 7
			}
			next = zeroTime.UnixMilli() + int64(days*24)*time.Hour.Milliseconds() +
				int64(policy.GetSchedule().GetFixTimeMinute())*time.Minute.Milliseconds()
		}
	}
	return next
}
