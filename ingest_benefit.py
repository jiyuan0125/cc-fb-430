#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ingest_benefit.py

把 drops 目录下按推送窗口切分的权益事件 jsonl 全量吃入，按 occurred_at
重放每个权益的生命周期，产出：
  - entitlements.db   benefits(benefit, member, status, updated_at)，一权益一行
  - invalid_moves.csv 好行但迁移非法的事件
  - bad_rows.csv      坏行（无法解析 / 字段不合法）
  - run_report.txt    五项计数
  - notes.txt         判定与终值口径说明

每次运行全量重建全部产物，排序规则确定，重复执行结果不变（幂等）。
"""

import argparse
import csv
import glob
import json
import os
import re
import sqlite3
from datetime import datetime

VALID_EVENTS = ("发放", "转赠", "核销", "过期")
TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
TS_FMT = "%Y-%m-%d %H:%M:%S"
EXPECTED_KEYS = {"benefit", "member", "event", "occurred_at"}

# 终态
HELD = "在持"
REDEEMED = "已核销"
EXPIRED = "已过期"

NOTES = """口径说明（ingest_benefit.py）
================================

一、读取与去重
1. 按文件名排序依次读取 drops 下 push_*.jsonl，行号为文件内从 1 开始的物理行号。
2. 重推行与首发行逐字一致：以去掉行尾换行后的整行原文为键，按（文件名, 行号）
   顺序首次出现者生效，之后出现的副本一律不生效，既不算坏行也不参与重放，
   仅累加 repeats 计数。
3. events_read 统计所有非空物理行（含重推、坏行）。

二、坏行口径（bad_rows.csv，记 file/line/raw 原文）
满足任一条件即坏行，一行只计一次，不进入后续重放：
1. JSON 无法解析，或解析结果不是含且仅含四个约定键的对象；
2. event 不属于 发放/转赠/核销/过期 四种；
3. member 不是纯数字组成的非空字符串（带字母等即坏）；
4. benefit 不是非空字符串（空串即坏）；
5. occurred_at 不符合 "YYYY-MM-DD HH:MM:SS" 格式或不是合法时刻。

三、重放顺序
窗口按推送时刻切、事件按发生时刻算账，因此不按文件推送顺序处理，而是把每个
权益的好行按（occurred_at, 文件名序号, 文件内行号）升序重放。occurred_at 为
定宽字符串，字典序即时间序；同一权益同一时刻的事件以推送窗口先后、再以行号
决胜，保证结果确定。

四、状态机与非法迁移（invalid_moves.csv，reason 四种）
权益状态只有：未发放（初始）、在持（记当前持有人）、已核销、已过期。
- 发放是唯一入口：未发放状态下只有 发放 合法，发放后状态=在持、持有人=该
  member；未发放状态下出现 转赠/核销/过期，reason=未发放先动作。
- 在持：转赠 -> 持有人改为受赠人（转赠事件的 member 即受赠人；受赠人与当前
  持有人相同视为合法空操作）；核销 -> 已核销；过期 -> 已过期。
  说明：核销事件的 member 是操作人而非持有人（数据中有 15 例由从未出现在该
  权益发放/转赠链上的会员执行核销，过期事件则全部由当前持有人署名），因此
  核销是否合法只看权益是否在持，不校验操作人，也不因核销改变持有人。
- 已核销是终点，状态与持有人冻结：
  * 再来 核销，reason=已核销再核销；
  * 再来 转赠 或 过期（含 occurred_at 晚于核销的迟到过期通知），
    reason=核销后流转。
- 已过期是终点，状态与持有人冻结：之后到达的事件不生效；本数据中过期后
  出现的均为 核销（含推送晚到、但 occurred_at 早于核销的通知，重放按发生
  时刻即判为先过期后核销），reason=过期后核销。
非法事件不改变状态，其后的事件继续按同一状态机判定。
去重后本数据不存在同一权益的多次发放；若出现，重复发放同样拒绝，归口
未发放先动作。

五、终值与落表
1. 只有进入过有效状态的权益在 benefits 表落一行；从未发放的权益（其动作全部
   落在 未发放先动作）不入库。
2. status：在持 / 已核销 / 已过期。
3. member：重放后最后有资格持有该权益的会员；核销、过期为终点，持有人冻结为
   终态事件发生时的持有人，终态后的事件不能再改持有人。
4. updated_at：终值对应事件的 occurred_at，即在持权益取最后一次生效的发放/
   转赠时刻，已核销/已过期权益取核销/过期事件的发生时刻。

六、幂等
每次运行先删除再全量重建数据库与两份 CSV、报告和本说明；所有输出均按确定键
排序（表按 benefit，CSV 按 文件名序号+行号），重复执行内容不变。
"""


def parse_args():
    ap = argparse.ArgumentParser(description="权益事件入库与重放")
    ap.add_argument("--drops", default="drops", help="jsonl 所在目录")
    ap.add_argument("--outdir", default=".", help="产物输出目录")
    return ap.parse_args()


def is_bad(obj):
    """返回 True 表示该（已成功 JSON 解析的）对象是坏行。"""
    if not isinstance(obj, dict) or set(obj.keys()) != EXPECTED_KEYS:
        return True
    benefit = obj.get("benefit")
    member = obj.get("member")
    event = obj.get("event")
    ts = obj.get("occurred_at")
    if not (isinstance(benefit, str) and benefit):
        return True
    if not (isinstance(member, str) and member.isdigit()):
        return True
    if event not in VALID_EVENTS:
        return True
    if not (isinstance(ts, str) and TS_RE.match(ts)):
        return True
    try:
        datetime.strptime(ts, TS_FMT)
    except ValueError:
        return True
    return False


def invalid_reason(status, event):
    if status is None:
        return "未发放先动作"
    if status == EXPIRED:
        return "过期后核销"
    if status == REDEEMED:
        return "已核销再核销" if event == "核销" else "核销后流转"
    # status == 在持 时仅可能因重复发放走到这里（本数据去重后不存在）
    return "未发放先动作"


def main():
    args = parse_args()
    os.makedirs(args.outdir, exist_ok=True)
    files = sorted(glob.glob(os.path.join(args.drops, "push_*.jsonl")))

    events_read = 0
    repeats = 0
    seen_raw = set()
    bad_rows = []        # (file_index, file_name, line_no, raw)
    good_events = []     # (file_index, file_name, line_no, obj)

    for fi, path in enumerate(files):
        fname = os.path.basename(path)
        with open(path, "r", encoding="utf-8", newline="") as fh:
            for ln, line in enumerate(fh, 1):
                raw = line.rstrip("\r\n")
                if not raw.strip():
                    continue
                events_read += 1
                if raw in seen_raw:
                    repeats += 1
                    continue
                seen_raw.add(raw)
                try:
                    obj = json.loads(raw)
                except json.JSONDecodeError:
                    bad_rows.append((fi, fname, ln, raw))
                    continue
                if is_bad(obj):
                    bad_rows.append((fi, fname, ln, raw))
                else:
                    good_events.append((fi, fname, ln, obj))

    # 按权益分组，按发生时刻（+来源位置决胜）重放
    by_benefit = {}
    for fi, fname, ln, obj in good_events:
        by_benefit.setdefault(obj["benefit"], []).append((fi, fname, ln, obj))

    invalid_moves = []  # (fi, fname, ln, obj, reason)
    landed = {}         # benefit -> (member, status, updated_at)

    for benefit in sorted(by_benefit):
        records = sorted(
            by_benefit[benefit],
            key=lambda r: (r[3]["occurred_at"], r[0], r[2]),
        )
        status = None
        holder = None
        updated_at = None
        for fi, fname, ln, obj in records:
            event = obj["event"]
            member = obj["member"]
            ts = obj["occurred_at"]
            if status is None:
                if event == "发放":
                    status, holder, updated_at = HELD, member, ts
                else:
                    invalid_moves.append(
                        (fi, fname, ln, obj, invalid_reason(status, event))
                    )
            elif status == HELD:
                if event == "转赠":
                    holder, updated_at = member, ts
                elif event == "核销":
                    status, updated_at = REDEEMED, ts
                elif event == "过期":
                    status, updated_at = EXPIRED, ts
                else:  # 重复发放，本数据不出现
                    invalid_moves.append(
                        (fi, fname, ln, obj, invalid_reason(status, event))
                    )
            else:  # 已核销 / 已过期，终态冻结
                invalid_moves.append(
                    (fi, fname, ln, obj, invalid_reason(status, event))
                )
        if status is not None:
            landed[benefit] = (holder, status, updated_at)

    bad_rows.sort(key=lambda r: (r[0], r[2]))
    invalid_moves.sort(key=lambda r: (r[0], r[2]))

    # ---- 全量重建产物 ----
    db_path = os.path.join(args.outdir, "entitlements.db")
    if os.path.exists(db_path):
        os.remove(db_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "CREATE TABLE benefits ("
            "benefit TEXT PRIMARY KEY, member TEXT, status TEXT, updated_at TEXT)"
        )
        conn.executemany(
            "INSERT INTO benefits (benefit, member, status, updated_at)"
            " VALUES (?, ?, ?, ?)",
            [(b, v[0], v[1], v[2]) for b, v in sorted(landed.items())],
        )
        conn.commit()
    finally:
        conn.close()

    with open(
        os.path.join(args.outdir, "invalid_moves.csv"),
        "w", encoding="utf-8", newline="",
    ) as fh:
        w = csv.writer(fh)
        w.writerow(["benefit", "member", "event", "occurred_at", "reason"])
        for fi, fname, ln, obj, reason in invalid_moves:
            w.writerow([
                obj["benefit"], obj["member"], obj["event"],
                obj["occurred_at"], reason,
            ])

    with open(
        os.path.join(args.outdir, "bad_rows.csv"),
        "w", encoding="utf-8", newline="",
    ) as fh:
        w = csv.writer(fh)
        w.writerow(["file", "line", "raw"])
        for fi, fname, ln, raw in bad_rows:
            w.writerow([fname, ln, raw])

    with open(os.path.join(args.outdir, "run_report.txt"), "w", encoding="utf-8") as fh:
        fh.write(f"events_read {events_read}\n")
        fh.write(f"benefits_landed {len(landed)}\n")
        fh.write(f"invalid_moves {len(invalid_moves)}\n")
        fh.write(f"bad_rows {len(bad_rows)}\n")
        fh.write(f"repeats {repeats}\n")

    with open(os.path.join(args.outdir, "notes.txt"), "w", encoding="utf-8") as fh:
        fh.write(NOTES)


if __name__ == "__main__":
    main()
