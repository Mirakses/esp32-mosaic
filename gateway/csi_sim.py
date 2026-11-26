#!/usr/bin/env python3
"""
csi_sim.py — synthetic csi_raw node, for testing the gateway and /viz without
hardware. Streams 64-subcarrier amplitude frames over WebSocket exactly like
firmware/csi-node: a static multipath channel plus noise, a person breathing
(15/min by default) and walking around for 8 s of every minute.

  python3 csi_sim.py                       # ws://127.0.0.1:9000/ws, node csi-sim
  python3 csi_sim.py --host 192.168.1.10 --node sim-2 --token s3cret
"""
import argparse
import asyncio
import base64
import json
import math
import random
import time

import aiohttp


def make_channel(rng):
    """Frequency response of a few static reflection paths (amplitude only)."""
    paths = [(rng.uniform(0.3, 1.0), rng.uniform(0, 60), rng.uniform(0, 6.28)) for _ in range(5)]
    def amp(k, extra=()):
        re = im = 0.0
        for a, delay, ph in list(paths) + list(extra):
            phi = ph - 2 * math.pi * k * delay / 64
            re += a * math.cos(phi)
            im += a * math.sin(phi)
        return math.hypot(re, im)
    return amp


async def run(args):
    rng = random.Random(1)
    channel = make_channel(rng)
    path = "/ws" + (f"?token={args.token}" if args.token else "")
    url = f"ws://{args.host}:{args.port}{path}"
    period = 1.0 / args.hz
    t0 = time.monotonic()
    async with aiohttp.ClientSession() as s:
        async with s.ws_connect(url) as ws:
            print(f"streaming to {url} as {args.node} at {args.hz} Hz (Ctrl+C to stop)")
            batch, n = [], 0
            while True:
                t = time.monotonic() - t0
                # a body walks around for the last `move` seconds of every `cycle`
                moving = (t % args.cycle) > args.cycle - args.move
                extra = []
                if moving:
                    extra = [(0.6, 20 + 15 * math.sin(t * 2.1), t * 9.0)]
                # breathing: a person sitting still — a tiny periodic path change
                extra.append((0.08, 30, 0.8 * math.sin(2 * math.pi * args.breath_bpm / 60 * t)))
                amps = bytearray(64)
                for k in range(64):
                    sub = k if k < 32 else k - 64
                    if sub == 0 or abs(sub) > 26:
                        continue  # DC / guard subcarriers carry nothing
                    a = channel(sub, extra) * 25 + rng.gauss(0, 0.6)
                    amps[k] = max(0, min(255, int(a)))
                batch.append([int(t * 1000), -48 + rng.randint(-2, 2), base64.b64encode(bytes(amps)).decode()])
                if len(batch) == args.batch:
                    env = {"v": 1, "node": args.node, "type": "csi_raw", "ts": int(time.time()),
                           "payload": {"n_sub": 64, "hz": args.hz, "channel": 6, "dropped": 0, "frames": batch}}
                    await ws.send_str(json.dumps(env))
                    batch = []
                    try:
                        await asyncio.wait_for(ws.receive(), timeout=0.001)  # drain acks
                    except asyncio.TimeoutError:
                        pass
                n += 1
                await asyncio.sleep(max(0.0, t0 + n * period - time.monotonic()))


def main():
    ap = argparse.ArgumentParser(description="synthetic csi_raw node")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--node", default="csi-sim")
    ap.add_argument("--token", default="")
    ap.add_argument("--hz", type=int, default=100)
    ap.add_argument("--batch", type=int, default=10)
    ap.add_argument("--cycle", type=float, default=60, help="seconds per walk cycle")
    ap.add_argument("--move", type=float, default=8, help="seconds of walking per cycle")
    ap.add_argument("--breath-bpm", type=float, default=15, help="simulated breathing rate")
    try:
        asyncio.run(run(ap.parse_args()))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
