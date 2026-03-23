/**
 * Hardware detection and fingerprinting for GenDB.
 * Used to tag code versions with hardware-specific timing results
 * and detect hardware changes between runs.
 */

import { createHash } from "crypto";
import { execSync } from "child_process";

/**
 * Detect current hardware characteristics.
 * Returns { cpu_cores, simd, total_memory_gb, disk_type, l3_cache_mb }.
 */
export function detectHardware() {
  const hw = {
    cpu_cores: 1,
    simd: "none",
    total_memory_gb: 0,
    disk_type: "unknown",
    l3_cache_mb: 0,
  };

  try {
    hw.cpu_cores = parseInt(execSync("nproc", { stdio: "pipe" }).toString().trim(), 10) || 1;
  } catch {}

  try {
    const flags = execSync("grep -m1 '^flags' /proc/cpuinfo", { stdio: "pipe" }).toString();
    if (flags.includes("avx512")) hw.simd = "avx512";
    else if (flags.includes("avx2")) hw.simd = "avx2";
    else if (flags.includes("avx")) hw.simd = "avx";
    else if (flags.includes("neon")) hw.simd = "neon";
    else if (flags.includes("sse4")) hw.simd = "sse4";
  } catch {}

  try {
    const memKb = execSync("grep MemTotal /proc/meminfo", { stdio: "pipe" }).toString();
    const match = memKb.match(/(\d+)/);
    if (match) hw.total_memory_gb = Math.round(parseInt(match[1], 10) / (1024 * 1024));
  } catch {}

  try {
    // Check if root disk is rotational
    const rota = execSync("lsblk -d -o name,rota 2>/dev/null | tail -n +2", { stdio: "pipe" }).toString();
    const lines = rota.trim().split("\n").filter(Boolean);
    if (lines.length > 0) {
      // Check the first disk
      const firstDisk = lines[0].trim();
      if (firstDisk.endsWith("0")) hw.disk_type = "ssd";
      else if (firstDisk.endsWith("1")) hw.disk_type = "hdd";
      // Try to detect NVMe
      if (firstDisk.startsWith("nvme")) hw.disk_type = "nvme";
    }
  } catch {}

  try {
    const lscpu = execSync("lscpu 2>/dev/null", { stdio: "pipe" }).toString();
    const l3Match = lscpu.match(/L3 cache:\s*([\d.]+)\s*(K|M|G)/i);
    if (l3Match) {
      const val = parseFloat(l3Match[1]);
      const unit = l3Match[2].toUpperCase();
      if (unit === "K") hw.l3_cache_mb = Math.round(val / 1024);
      else if (unit === "M") hw.l3_cache_mb = Math.round(val);
      else if (unit === "G") hw.l3_cache_mb = Math.round(val * 1024);
    }
  } catch {}

  return hw;
}

/**
 * Generate a 12-character hex fingerprint from hardware attributes.
 * Deterministic: same hardware → same fingerprint.
 */
export function getHardwareFingerprint(hw) {
  const sorted = Object.keys(hw).sort().reduce((o, k) => { o[k] = hw[k]; return o; }, {});
  return createHash("sha256")
    .update(JSON.stringify(sorted))
    .digest("hex")
    .slice(0, 12);
}

/**
 * Check if a fingerprint matches the current hardware.
 */
export function hardwareMatches(fingerprint, hw) {
  return getHardwareFingerprint(hw) === fingerprint;
}
