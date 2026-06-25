package config

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Size 是磁盘/内存大小，YAML 可写 "100MB"/"1GB"/"512KiB"/"104857600"。
// 后缀按二进制解释（KB=KiB=1024，MB=MiB=1024²，…），与 lumberjack/Go 惯例一致；
// 裸数字按字节。便于和 lumberjack 的 MaxSize（MB=1024²）对齐。
type Size int64

func (s *Size) UnmarshalYAML(value *yaml.Node) error {
	// 先试字符串（带后缀），失败再试裸数字（字节）。
	var str string
	if err := value.Decode(&str); err == nil {
		n, err := parseSize(str)
		if err != nil {
			return err
		}
		*s = Size(n)
		return nil
	}
	var n int64
	if err := value.Decode(&n); err != nil {
		return fmt.Errorf("size: expected string or int, got %v", value.Tag)
	}
	*s = Size(n)
	return nil
}

// Bytes 返回字节数。
func (s Size) Bytes() int64 { return int64(s) }

// Megabytes 返回 MB 数（二进制，1MB=1024²），供 lumberjack MaxSize 用。
func (s Size) Megabytes() int { return int(int64(s) / (1 << 20)) }

func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}
	i := 0
	for i < len(s) && (s[i] == '.' || s[i] == '-' || s[i] == '+' || (s[i] >= '0' && s[i] <= '9')) {
		i++
	}
	numStr := s[:i]
	suffix := strings.ToUpper(strings.TrimSpace(s[i:]))
	n, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("parse size %q: %w", s, err)
	}
	var mult int64
	switch suffix {
	case "", "B":
		mult = 1
	case "K", "KB", "KIB":
		mult = 1 << 10
	case "M", "MB", "MIB":
		mult = 1 << 20
	case "G", "GB", "GIB":
		mult = 1 << 30
	case "T", "TB", "TIB":
		mult = 1 << 40
	default:
		return 0, fmt.Errorf("unknown size suffix %q in %q", suffix, s)
	}
	return int64(n * float64(mult)), nil
}

// dayRe 匹配 "<数>d"/"<数>day"/"<数>days"（大小写不敏感），转成小时——
// time.ParseDuration 只支持到 h，不支持天。
var dayRe = regexp.MustCompile(`(?i)([0-9]+(?:\.[0-9]+)?)\s*(?:days?|d)`)

// parseDuration 扩展 time.ParseDuration：额外支持天（d/day/days = 24h），
// 可与标准单位组合，如 "1d30m"、"7days"、"1.5d"。
func parseDuration(s string) (time.Duration, error) {
	s = dayRe.ReplaceAllStringFunc(s, func(m string) string {
		sub := dayRe.FindStringSubmatch(m)
		n, _ := strconv.ParseFloat(sub[1], 64)
		return strconv.FormatFloat(n*24, 'f', -1, 64) + "h"
	})
	return time.ParseDuration(s)
}

// Duration 是时间段，YAML 可写 "5s"/"10m"/"1h"/"500ms"/"1d"/"7days"/"1d30m"；
// 裸数字按秒。
type Duration time.Duration

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var str string
	if err := value.Decode(&str); err == nil {
		if dd, err := parseDuration(str); err == nil {
			*d = Duration(dd)
			return nil
		}
		// yaml 把裸数字 30 解成 "30"；按秒处理。
		if n, err := strconv.ParseFloat(str, 64); err == nil {
			*d = Duration(time.Duration(n * float64(time.Second)))
			return nil
		}
		return fmt.Errorf("parse duration %q: invalid", str)
	}
	var n int64
	if err := value.Decode(&n); err != nil {
		return fmt.Errorf("duration: expected string or int, got %v", value.Tag)
	}
	*d = Duration(time.Duration(n) * time.Second) // 裸数字 = 秒
	return nil
}

// D 返回标准 time.Duration。
func (d Duration) D() time.Duration { return time.Duration(d) }
