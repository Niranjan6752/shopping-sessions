//! Shopping Sessions pipeline: implements rules.md exactly.
//! Reads /data CSVs, writes /output/user_daily and /output/campaign_daily as Parquet.

use hashbrown::HashTable;
use memchr::{memchr, memchr_iter};
use rayon::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::Instant;

type S = &'static [u8];

const DAY: i64 = 86_400_000;
const GAP: i64 = 1_800_000;
const WEEK: i64 = 7 * DAY;
const LATE: i64 = DAY;
const CHUNK: usize = 4 << 20;
const SHARDS: usize = 256;

// ---------------------------------------------------------------- utilities

#[inline]
fn fmix(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51afd7ed558ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
    h ^= h >> 33;
    h
}

#[inline]
fn hash(b: &[u8]) -> u64 {
    use std::hash::Hasher;
    let mut h = rustc_hash::FxHasher::default();
    h.write(b);
    fmix(h.finish() ^ (b.len() as u64).wrapping_mul(0x9e3779b97f4a7c15))
}

fn log(t0: Instant, msg: &str) {
    if std::env::var_os("QUIET").is_none() {
        eprintln!("[{:7.3}s] {}", t0.elapsed().as_secs_f64(), msg);
    }
}

fn map_file(p: &Path) -> S {
    let f = std::fs::File::open(p).unwrap_or_else(|e| panic!("open {}: {e}", p.display()));
    let len = f.metadata().map(|m| m.len()).unwrap_or(0);
    if len == 0 {
        return &[];
    }
    let m = unsafe { memmap2::Mmap::map(&f) }.expect("mmap");
    let m: &'static memmap2::Mmap = Box::leak(Box::new(m));
    &m[..]
}

fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false) && p.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

/// Split every file (minus its header line) into newline-aligned chunks.
fn chunks(dir: &Path) -> Vec<S> {
    let files: Vec<S> = csv_files(dir).par_iter().map(|p| map_file(p)).collect();
    let mut out = Vec::new();
    for data in files {
        let start = match memchr(b'\n', data) {
            Some(p) => p + 1,
            None => continue, // header only (or empty)
        };
        let body = &data[start..];
        let mut s = 0;
        while s < body.len() {
            let mut e = (s + CHUNK).min(body.len());
            if e < body.len() {
                e = match memchr(b'\n', &body[e..]) {
                    Some(p) => e + p + 1,
                    None => body.len(),
                };
            }
            out.push(&body[s..e]);
            s = e;
        }
    }
    out
}

#[inline]
#[allow(dead_code)]
fn for_each_line(chunk: S, mut f: impl FnMut(S)) {
    let mut s = 0;
    for p in memchr_iter(b'\n', chunk) {
        f(&chunk[s..p]);
        s = p + 1;
    }
    if s < chunk.len() {
        f(&chunk[s..]);
    }
}

#[inline]
#[allow(dead_code)]
fn unquote(f: S) -> Option<S> {
    if memchr(b'"', f).is_none() {
        return Some(f);
    }
    let n = f.len();
    if n >= 2 && f[0] == b'"' && f[n - 1] == b'"' && memchr(b'"', &f[1..n - 1]).is_none() {
        Some(&f[1..n - 1])
    } else {
        None
    }
}

/// Split a line into exactly N fields per the CSV dialect. false = malformed.
#[inline]
#[allow(dead_code)]
fn fields<const N: usize>(line: S, out: &mut [S; N]) -> bool {
    let quoted = memchr(b'"', line).is_some();
    let mut n = 0;
    let mut s = 0;
    let mut it = memchr_iter(b',', line);
    loop {
        let (e, last) = match it.next() {
            Some(p) => (p, false),
            None => (line.len(), true),
        };
        if n == N {
            return false;
        }
        let mut f = &line[s..e];
        if quoted {
            match unquote(f) {
                Some(x) => f = x,
                None => return false,
            }
        }
        out[n] = f;
        n += 1;
        if last {
            break;
        }
        s = e + 1;
    }
    n == N
}


/// Bitmasks of '\n', ',' and '"' for 64 bytes starting at p (SSE2, always on x86_64).
#[inline(always)]
fn classify(block: &[u8; 64]) -> (u64, u64, u64) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use std::arch::x86_64::*;
        let nl = _mm256_set1_epi8(b'\n' as i8);
        let cm = _mm256_set1_epi8(b',' as i8);
        let qt = _mm256_set1_epi8(b'"' as i8);
        let v0 = _mm256_loadu_si256(block.as_ptr() as *const __m256i);
        let v1 = _mm256_loadu_si256(block.as_ptr().add(32) as *const __m256i);
        let m = |a: __m256i, b: __m256i| -> u64 {
            (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v0, a)) as u32 as u64)
                | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(v1, b)) as u32 as u64) << 32)
        };
        return (m(nl, nl), m(cm, cm), m(qt, qt));
    }
    #[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
    unsafe {
        use std::arch::x86_64::*;
        let nl = _mm_set1_epi8(b'\n' as i8);
        let cm = _mm_set1_epi8(b',' as i8);
        let qt = _mm_set1_epi8(b'"' as i8);
        let (mut a, mut b, mut c) = (0u64, 0u64, 0u64);
        for i in 0..4 {
            let v = _mm_loadu_si128(block.as_ptr().add(i * 16) as *const __m128i);
            a |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, nl)) as u16 as u64) << (i * 16);
            b |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, cm)) as u16 as u64) << (i * 16);
            c |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, qt)) as u16 as u64) << (i * 16);
        }
        (a, b, c)
    }
    #[cfg(not(target_arch = "x86_64"))]
    #[allow(unreachable_code)]
    {
        let (mut a, mut b, mut c) = (0u64, 0u64, 0u64);
        for (i, &x) in block.iter().enumerate() {
            a |= ((x == b'\n') as u64) << i;
            b |= ((x == b',') as u64) << i;
            c |= ((x == b'"') as u64) << i;
        }
        (a, b, c)
    }
}

/// Call f with the N unquoted fields of every well-formed line in chunk (CSV dialect of rules.md).
/// Walks only comma and newline bits; quotes are counted with popcount.
#[inline(always)]
fn scan<const N: usize>(chunk: S, mut f: impl FnMut(&[S; N])) {
    let len = chunk.len();
    let mut ends = [0usize; 8]; // end offsets of fields (commas); slot N.. absorbs extras
    let mut nc = 0usize; // commas seen in this line
    let mut start = 0usize; // line start
    let mut q_line = 0u32; // cumulative quote count at line start
    let mut q_base = 0u32; // cumulative quote count at block start
    let mut out: [S; N] = [&[]; N];
    // A line is well formed when it has N fields and its only quotes are pairs wrapping whole
    // fields: then the quote count is exactly 2 per wrapped field.
    let mut emit = |start: usize, end: usize, nc: usize, nq: u32, ends: &[usize; 8]| {
        if nc != N - 1 {
            return;
        }
        let mut s = start;
        let mut wrapped = 0u32;
        for k in 0..N {
            let e = if k == N - 1 { end } else { ends[k] };
            let mut fld = &chunk[s..e];
            if nq != 0 && fld.len() >= 2 && fld[0] == b'"' && fld[fld.len() - 1] == b'"' {
                fld = &fld[1..fld.len() - 1];
                wrapped += 2;
            }
            out[k] = fld;
            s = e + 1;
        }
        if wrapped == nq {
            f(&out);
        }
    };
    let mut base = 0usize;
    let mut tmp = [0u8; 64];
    while base < len {
        let block: &[u8; 64] = if base + 64 <= len {
            chunk[base..base + 64].try_into().unwrap()
        } else {
            tmp = [0u8; 64];
            tmp[..len - base].copy_from_slice(&chunk[base..]);
            &tmp
        };
        let (mn, mc, mq) = classify(block);
        let mut m = mn | mc;
        while m != 0 {
            let bit = m & m.wrapping_neg();
            m ^= bit;
            let pos = base + bit.trailing_zeros() as usize;
            if mc & bit != 0 {
                ends[nc.min(7)] = pos;
                nc += 1;
            } else {
                let q = q_base + (mq & (bit - 1)).count_ones();
                emit(start, pos, nc, q - q_line, &ends);
                q_line = q;
                start = pos + 1;
                nc = 0;
            }
        }
        q_base += mq.count_ones();
        base += 64;
    }
    if start < len {
        emit(start, len, nc, q_base - q_line, &ends);
    }
}

// ---------------------------------------------------------------- timestamps

#[inline]
fn d2(b: &[u8], i: usize) -> Option<i64> {
    let a = b[i].wrapping_sub(b'0');
    let c = b[i + 1].wrapping_sub(b'0');
    if a < 10 && c < 10 {
        Some((a * 10 + c) as i64)
    } else {
        None
    }
}

#[cfg(test)]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

#[inline]
#[cfg(test)]
fn leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

#[cfg(test)]
fn dim(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if leap(y) {
                29
            } else {
                28
            }
        }
    }
}

const CUM_DAYS: [[i64; 13]; 2] = [
    [0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334],
    [0, 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335],
];
const MONTH_DAYS: [[i64; 13]; 2] = [
    [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
    [0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
];

/// Days from 1970-01-01 to (y, m, d), for 0 <= y <= 9999 and a valid date.
#[inline]
fn days_ymd(y: i64, m: i64, d: i64, lp: usize) -> i64 {
    let p = y - 1; // leap days before year y, counted from year 0 (year 0 is a leap year)
    let leaps = if y > 0 { p / 4 - p / 100 + p / 400 + 1 } else { 0 };
    y * 365 + leaps + CUM_DAYS[lp][m as usize] + d - 1 - 719_528
}

/// 8 ASCII digits (first digit in the lowest byte) to their value, or None.
#[inline]
fn digits8(x: u64) -> Option<u64> {
    let t = x ^ 0x3030_3030_3030_3030;
    if (t.wrapping_add(0x7676_7676_7676_7676) | t) & 0x8080_8080_8080_8080 != 0 {
        return None;
    }
    let v = t.wrapping_mul(10).wrapping_add(t >> 8);
    let v = ((v & 0x0000_00FF_0000_00FF).wrapping_mul(100 + (1_000_000 << 32))
        .wrapping_add(((v >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000 << 32))))
        >> 32;
    Some(v)
}

/// F1 / F2: "YYYY-MM-DDTHH:MM:SS" then "Z" or "+HH:MM" / "-HH:MM".
#[inline]
fn parse_iso(b: &[u8]) -> Option<i64> {
    let n = b.len();
    let lo = u64::from_le_bytes(b[0..8].try_into().unwrap()); // YYYY-MM-
    let hi = u64::from_le_bytes(b[8..16].try_into().unwrap()); // DDTHH:MM
    const LO_SEP: u64 = 0xFF00_00FF_0000_0000;
    const HI_SEP: u64 = 0x0000_FF00_00FF_0000;
    if lo & LO_SEP != 0x2D00_002D_0000_0000 || hi & HI_SEP != 0x0000_3A00_0054_0000 || b[16] != b':' {
        return None;
    }
    let l = (lo & !LO_SEP) | (0x3030_3030_3030_3030 & LO_SEP);
    let h = (hi & !HI_SEP) | (0x3030_3030_3030_3030 & HI_SEP);
    let lt = l ^ 0x3030_3030_3030_3030;
    let ht = h ^ 0x3030_3030_3030_3030;
    if ((lt.wrapping_add(0x7676_7676_7676_7676) | lt) | (ht.wrapping_add(0x7676_7676_7676_7676) | ht)) & 0x8080_8080_8080_8080 != 0
    {
        return None;
    }
    let s1 = b[17].wrapping_sub(b'0');
    let s2 = b[18].wrapping_sub(b'0');
    if s1 > 9 || s2 > 9 {
        return None;
    }
    let by = |x: u64, i: u32| ((x >> (8 * i)) & 0xFF) as i64;
    let y = by(lt, 0) * 1000 + by(lt, 1) * 100 + by(lt, 2) * 10 + by(lt, 3);
    let mo = by(lt, 5) * 10 + by(lt, 6);
    let d = by(ht, 0) * 10 + by(ht, 1);
    let hh = by(ht, 3) * 10 + by(ht, 4);
    let mi = by(ht, 6) * 10 + by(ht, 7);
    let ss = s1 as i64 * 10 + s2 as i64;
    let lp = ((y % 4 == 0 && y % 100 != 0) || y % 400 == 0) as usize;
    if !(1..=12).contains(&mo) || d < 1 || d > MONTH_DAYS[lp][mo as usize] || hh > 23 || mi > 59 || ss > 59 {
        return None;
    }
    let mut secs = days_ymd(y, mo, d, lp) * 86400 + hh * 3600 + mi * 60 + ss;
    if n == 20 {
        if b[19] != b'Z' {
            return None;
        }
    } else {
        let sign = match b[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        let oh = d2(b, 20)?;
        if b[22] != b':' {
            return None;
        }
        let om = d2(b, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        secs -= sign * (oh * 3600 + om * 60);
    }
    Some(secs * 1000)
}

/// Parse an F1/F2/F3 timestamp to epoch ms. None = bad.
#[inline]
fn parse_ts(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n == 0 {
        return None;
    }
    if (n == 20 || n == 25) && b[4] == b'-' {
        return parse_iso(b);
    }
    // F3: ASCII digits only
    if n > 8 && n <= 16 {
        // two overlapping in-bounds loads: the last 8 digits, and the first n-8 shifted up
        // with '0' filled in below them (no copy through memory, so no store-forwarding stall)
        let c = digits8(u64::from_le_bytes(b[n - 8..].try_into().unwrap()))?;
        let sh = 8 * (16 - n) as u32;
        let x = u64::from_le_bytes(b[..8].try_into().unwrap());
        let hi = if sh == 0 { x } else { (x << sh) | (0x3030_3030_3030_3030u64 >> (64 - sh)) };
        let a = digits8(hi)?;
        return Some((a * 100_000_000 + c) as i64);
    }
    if n <= 8 {
        let mut buf = [b'0'; 8];
        buf[8 - n..].copy_from_slice(b);
        return digits8(u64::from_le_bytes(buf)).map(|v| v as i64);
    }
    let mut v: u64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(d as u64)?;
    }
    if v > i64::MAX as u64 {
        return None;
    }
    Some(v as i64)
}

/// Digits only, 1..=10_000_000.
#[inline]
fn parse_price(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        v = (v * 10 + d as u64).min(1 << 40);
    }
    if (1..=10_000_000).contains(&v) {
        Some(v as u32)
    } else {
        None
    }
}

fn manifest_ts(json: &[u8], key: &str) -> i64 {
    let k = format!("\"{key}\"");
    let pos = json
        .windows(k.len())
        .position(|w| w == k.as_bytes())
        .unwrap_or_else(|| panic!("manifest missing {key}"));
    let rest = &json[pos + k.len()..];
    let colon = memchr(b':', rest).unwrap();
    let rest = &rest[colon + 1..];
    let q1 = memchr(b'"', rest).unwrap();
    let rest = &rest[q1 + 1..];
    let q2 = memchr(b'"', rest).unwrap();
    parse_ts(&rest[..q2]).expect("manifest timestamp")
}

// ---------------------------------------------------------------- small tables

struct Lookup {
    table: HashTable<(u64, S, u32)>,
}

impl Lookup {
    fn new() -> Self {
        Lookup { table: HashTable::new() }
    }
    fn insert(&mut self, k: S, v: u32) {
        let h = hash(k);
        if self.table.find(h, |e| e.1 == k).is_none() {
            self.table.insert_unique(h, (h, k, v), |e| e.0);
        }
    }
    #[inline]
    fn get(&self, k: &[u8]) -> Option<u32> {
        let h = hash(k);
        self.table.find(h, |e| e.1 == k).map(|e| e.2)
    }
}

fn small_csv(path: &Path) -> Vec<(S, S)> {
    let data = map_file(path);
    let mut v = Vec::new();
    let start = match memchr(b'\n', data) {
        Some(p) => p + 1,
        None => return v,
    };
    scan::<2>(&data[start..], |f| {
        if !f[0].is_empty() {
            v.push((f[0], f[1]));
        }
    });
    v
}

// ---------------------------------------------------------------- id codes

/// Top bit set: the ID did not fit the exact code and is a hash (original kept in `Side`).
const HBIT: u64 = 1 << 63;

static CODE: [u8; 256] = {
    let mut t = [0u8; 256];
    let mut i = 0;
    while i < 10 {
        t[b'0' as usize + i] = 1 + i as u8;
        i += 1;
    }
    let mut i = 0;
    while i < 26 {
        t[b'a' as usize + i] = 11 + i as u8;
        i += 1;
    }
    t
};

const POW37: [u64; 13] = {
    let mut t = [1u64; 13];
    let mut i = 1;
    while i < 13 {
        t[i] = t[i - 1] * 37;
        i += 1;
    }
    t
};

/// Exact, order-preserving 63-bit code of an ID made of 1 to 12 characters from [0-9a-z]:
/// base-37 digits, left aligned, 0 = past the end. So for two such IDs, comparing codes is
/// comparing bytes. Any other ID gets a 64-bit hash with HBIT set.
#[inline]
fn enc(s: &[u8]) -> u64 {
    let n = s.len();
    if n >= 1 && n <= 12 {
        let mut v = 0u64;
        let mut ok = true;
        for &c in s {
            let d = CODE[c as usize];
            ok &= d != 0;
            v = v * 37 + d as u64;
        }
        if ok {
            return v * POW37[12 - n];
        }
    }
    hash(s) | HBIT
}

fn dec(mut v: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(12);
    for i in (0..12).rev() {
        let d = (v / POW37[i]) as u8;
        v %= POW37[i];
        if d == 0 {
            break;
        }
        out.push(if d <= 10 { b'0' + d - 1 } else { b'a' + d - 11 });
    }
    out
}

/// Original text of hashed IDs.
type Side = hashbrown::HashMap<u64, S>;

#[inline]
fn dec_into(mut v: u64, out: &mut Vec<u8>) {
    for i in (0..12).rev() {
        let d = (v / POW37[i]) as u8;
        v %= POW37[i];
        if d == 0 {
            break;
        }
        out.push(if d <= 10 { b'0' + d - 1 } else { b'a' + d - 11 });
    }
}

fn id_bytes(code: u64, side: &Side) -> Vec<u8> {
    if code & HBIT == 0 {
        dec(code)
    } else {
        side[&code].to_vec()
    }
}

#[inline]
fn cmp_ids(a: u64, b: u64, side: &Side) -> std::cmp::Ordering {
    if (a | b) & HBIT == 0 {
        a.cmp(&b)
    } else {
        id_bytes(a, side).cmp(&id_bytes(b, side))
    }
}

#[inline]
fn shard(code: u64) -> usize {
    (fmix(code) >> 56) as usize
}

// ---------------------------------------------------------------- linked IDs

/// user code -> person key, where the key is the code of the person's name (the byte-wise
/// smallest of its IDs). IDs in no link line are their own person, so their key is their code.
struct People {
    shards: Vec<HashTable<(u64, u64)>>,
}

impl People {
    #[inline]
    fn key(&self, code: u64) -> u64 {
        match self.shards[shard(code)].find(fmix(code), |e| e.0 == code) {
            Some(e) => e.1,
            None => code,
        }
    }
}

/// Flat open-addressing map user code -> person key for IDs whose person is not themselves.
/// Built once, then read by every thread with software prefetch ahead of each lookup.
/// Codes are never 0 (an ID has at least one character), so 0 marks an empty slot.
struct PersonMap {
    slots: *mut [u64; 2],
    mask: usize,
}
unsafe impl Sync for PersonMap {}
unsafe impl Send for PersonMap {}

impl PersonMap {
    fn build(people: &People) -> PersonMap {
        let total: usize = people.shards.iter().map(|t| t.iter().filter(|e| e.0 != e.1).count()).sum();
        let n = (total * 2).next_power_of_two().max(1 << 12);
        let bytes = n * 16;
        let slots = unsafe {
            let p = libc::mmap(
                std::ptr::null_mut(),
                bytes,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            );
            assert!(p != libc::MAP_FAILED, "mmap failed");
            libc::madvise(p, bytes, libc::MADV_HUGEPAGE);
            p as *mut [u64; 2]
        };
        let map = PersonMap { slots, mask: n - 1 };
        // parallel insert: claim a slot's key with compare-and-swap, then write its value
        let cells = unsafe { std::slice::from_raw_parts(slots as *const std::sync::atomic::AtomicU64, n * 2) };
        people.shards.par_iter().for_each(|t| {
            for &(code, key) in t.iter() {
                if code == key {
                    continue; // a miss returns the code itself
                }
                let mut pos = (fmix(code) as usize) & map.mask;
                loop {
                    match cells[pos * 2].compare_exchange(0, code, Relaxed, Relaxed) {
                        Ok(_) => {
                            cells[pos * 2 + 1].store(key, Relaxed);
                            break;
                        }
                        Err(_) => pos = (pos + 1) & map.mask,
                    }
                }
            }
        });
        map
    }
    #[inline(always)]
    fn prefetch(&self, code: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::*;
            let pos = (fmix(code) as usize) & self.mask;
            _mm_prefetch(self.slots.add(pos) as *const i8, _MM_HINT_T0);
        }
    }
    #[inline(always)]
    fn key(&self, code: u64) -> u64 {
        let mut pos = (fmix(code) as usize) & self.mask;
        loop {
            let [k, v] = unsafe { *self.slots.add(pos) };
            if k == code {
                return v;
            }
            if k == 0 {
                return code;
            }
            pos = (pos + 1) & self.mask;
        }
    }
}

fn find(p: &mut [u32], mut x: u32) -> u32 {
    while p[x as usize] != x {
        let gp = p[p[x as usize] as usize];
        p[x as usize] = gp;
        x = gp;
    }
    x
}

fn parse_links(dir: &Path, threads: usize) -> (Vec<Vec<(u64, u64)>>, Vec<(u64, S)>) {
    let ch = chunks(dir);
    let res: Vec<(Vec<(u64, u64)>, Vec<(u64, S)>)> = run_par(
        ch.len(),
        threads,
        || (Vec::new(), Vec::new()),
        |(v, side), ci| {
            scan::<2>(ch[ci], |f| {
                if !f[0].is_empty() && !f[1].is_empty() {
                    let (a, b) = (enc(f[0]), enc(f[1]));
                    if a & HBIT != 0 {
                        side.push((a, f[0]));
                    }
                    if b & HBIT != 0 {
                        side.push((b, f[1]));
                    }
                    v.push((a, b));
                }
            });
        },
    );
    let mut side = Vec::new();
    let mut pairs = Vec::new();
    for (p, s) in res {
        pairs.push(p);
        side.extend(s);
    }
    (pairs, side)
}

fn build_people(pairs: Vec<Vec<(u64, u64)>>, side: &Side, threads: usize) -> People {
    // intern endpoints shard by shard
    let _ = threads;
    let per: Vec<Vec<Vec<u64>>> = pairs
        .par_iter()
        .map(|v| {
            let mut s: Vec<Vec<u64>> = vec![Vec::new(); SHARDS];
            for &(a, b) in v {
                s[shard(a)].push(a);
                s[shard(b)].push(b);
            }
            s
        })
        .collect();
    let mut tables: Vec<(HashTable<(u64, u64)>, Vec<u64>)> = (0..SHARDS)
        .into_par_iter()
        .map(|sh| {
            let mut t: HashTable<(u64, u64)> = HashTable::new();
            let mut ids: Vec<u64> = Vec::new();
            for part in &per {
                for &c in &part[sh] {
                    let h = fmix(c);
                    if t.find(h, |e| e.0 == c).is_none() {
                        t.insert_unique(h, (c, ids.len() as u64), |e| fmix(e.0));
                        ids.push(c);
                    }
                }
            }
            (t, ids)
        })
        .collect();
    drop(per);
    let mut offs = vec![0u64; SHARDS];
    let mut total = 0u64;
    for (i, (_, ids)) in tables.iter().enumerate() {
        offs[i] = total;
        total += ids.len() as u64;
    }
    let mut all: Vec<u64> = Vec::with_capacity(total as usize);
    for (_, ids) in &tables {
        all.extend_from_slice(ids);
    }
    tables.par_iter_mut().enumerate().for_each(|(i, (t, _))| {
        let o = offs[i];
        for e in t.iter_mut() {
            e.1 += o;
        }
    });
    let gid = |c: u64| -> u32 { tables[shard(c)].0.find(fmix(c), |e| e.0 == c).unwrap().1 as u32 };
    let edges: Vec<Vec<(u32, u32)>> = pairs.par_iter().map(|v| v.iter().map(|&(a, b)| (gid(a), gid(b))).collect()).collect();
    drop(pairs);
    let mut parent: Vec<u32> = (0..total as u32).collect();
    for v in &edges {
        for &(a, b) in v {
            let ra = find(&mut parent, a);
            let rb = find(&mut parent, b);
            if ra != rb {
                if ra < rb {
                    parent[rb as usize] = ra;
                } else {
                    parent[ra as usize] = rb;
                }
            }
        }
    }
    let mut root = vec![0u32; total as usize];
    for i in 0..total as u32 {
        root[i as usize] = find(&mut parent, i);
    }
    // name of each root: the byte-wise smallest member
    let mut name: Vec<u64> = all.clone();
    for i in 0..total as usize {
        let r = root[i] as usize;
        if cmp_ids(all[i], name[r], side).is_lt() {
            name[r] = all[i];
        }
    }
    let (root, name) = (&root, &name);
    let shards: Vec<HashTable<(u64, u64)>> = tables
        .into_par_iter()
        .map(|(mut t, _)| {
            for e in t.iter_mut() {
                e.1 = name[root[e.1 as usize] as usize];
            }
            t
        })
        .collect();
    People { shards }
}

// ---------------------------------------------------------------- parallel helpers

/// Run f(state, item) for every item in 0..n on `threads` threads that pull items from a
/// shared counter. Returns each thread's final state.
fn run_par<T: Send>(
    n: usize,
    threads: usize,
    init: impl Fn() -> T + Sync,
    f: impl Fn(&mut T, usize) + Sync,
) -> Vec<T> {
    let next = AtomicUsize::new(0);
    let (next, init, f) = (&next, &init, &f);
    std::thread::scope(|s| {
        let hs: Vec<_> = (0..threads)
            .map(|_| {
                s.spawn(move || {
                    let mut st = init();
                    loop {
                        let i = next.fetch_add(1, Relaxed);
                        if i >= n {
                            break;
                        }
                        f(&mut st, i);
                    }
                    st
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

fn parts<R>(n: usize, cap: usize) -> Vec<Vec<R>> {
    (0..n).map(|_| Vec::with_capacity(cap)).collect()
}


// ---------------------------------------------------------------- id maps

/// Product / campaign IDs: when every ID in the file is `prefix` + a canonical decimal
/// (no leading zero), look them up in a flat array; otherwise use a hash table. A query that
/// is not canonical cannot equal any canonical ID, so both paths are exact.
struct IdMap {
    prefix: &'static [u8],
    arr: Vec<u32>,
    hash: Lookup,
    fast: bool,
}

#[inline]
fn canon_num(s: &[u8], prefix: &[u8]) -> Option<usize> {
    if s.len() <= prefix.len() || s.len() > prefix.len() + 7 {
        return None;
    }
    for i in 0..prefix.len() {
        if s[i] != prefix[i] {
            return None;
        }
    }
    let d = &s[prefix.len()..];
    if d[0] == b'0' && d.len() > 1 {
        return None;
    }
    let mut v = 0usize;
    for &c in d {
        let x = c.wrapping_sub(b'0');
        if x >= 10 {
            return None;
        }
        v = v * 10 + x as usize;
    }
    Some(v)
}

impl IdMap {
    fn build(ids: &[(S, u32)], prefix: &'static [u8]) -> IdMap {
        let mut hash = Lookup::new();
        for &(k, v) in ids {
            hash.insert(k, v);
        }
        let nums: Option<Vec<usize>> = ids.iter().map(|&(k, _)| canon_num(k, prefix)).collect();
        let mut arr = Vec::new();
        let fast = match nums {
            Some(nums) => {
                let max = nums.iter().copied().max().unwrap_or(0);
                arr = vec![u32::MAX; max + 1];
                for (&n, &(k, _)) in nums.iter().zip(ids) {
                    if arr[n] == u32::MAX {
                        arr[n] = hash.get(k).unwrap();
                    }
                }
                true
            }
            None => false,
        };
        IdMap { prefix, arr, hash, fast }
    }
    #[inline]
    fn get(&self, s: &[u8]) -> Option<u32> {
        if self.fast {
            let n = canon_num(s, self.prefix)?;
            match self.arr.get(n) {
                Some(&v) if v != u32::MAX => Some(v),
                _ => None,
            }
        } else {
            self.hash.get(s)
        }
    }
}

// ---------------------------------------------------------------- records

/// Event payload in one word, ts in the high bits so (key, meta) sorts by time:
/// ts (ms since window_start, 31 bits) << 33 | typ (2) << 31 | cat (7) << 24 | price (24).
#[inline]
fn meta(ts: u32, typ: u8, cat: u32, price: u32) -> u64 {
    (ts as u64) << 33 | (typ as u64) << 31 | (cat as u64) << 24 | price as u64
}
#[inline]
fn m_ts(m: u64) -> u32 {
    (m >> 33) as u32
}
#[inline]
fn m_typ(m: u64) -> u8 {
    ((m >> 31) & 3) as u8
}
#[inline]
fn m_cat(m: u64) -> usize {
    ((m >> 24) & 0x7F) as usize
}
#[inline]
fn m_price(m: u64) -> i64 {
    (m & 0xFF_FFFF) as i64
}

/// An event row as parsed, kept for dedup. typ 0 in meta = the row is invalid.
#[derive(Clone, Copy)]
struct Raw {
    eid: u64,
    user: u64,
    ingest: i64,
    meta: u64,
}

/// A surviving event: its user code (before person resolution) or person key (after).
#[derive(Clone, Copy)]
struct Ev {
    key: u64,
    meta: u64,
}

#[derive(Clone, Copy)]
struct Clk {
    key: u64,
    cid: u64,
    ts: u32, // ms since window_start - 7 days
    camp: u32,
}

struct UserRow {
    key: u64,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: Option<u16>,
}

struct Ctx {
    ws: i64,
    cws: i64,
    day0: i64,
    ndays: usize,
    ncamp: usize, // organic = ncamp
    ncat: usize,
}

// per campaign-day: credited sessions, purchasing, revenue, assisted
type Acc = Vec<[i64; 4]>;

struct Sess {
    start: i64,
    rev: i64,
    purch: bool,
    d: usize,
}

struct Scratch {
    catsum: Vec<i64>,
    touched: Vec<u16>,
    sess: Vec<Sess>,
    ct: Vec<(u32, i64)>,
    ebuf: Vec<Ev>,
    cbuf: Vec<Clk>,
    cnt: Vec<usize>,
}

fn process_bucket(
    ctx: &Ctx,
    side: &Side,
    evs: &mut [Ev],
    clks: &mut [Clk],
    rows: &mut Vec<UserRow>,
    acc: &mut Acc,
    sc: &mut Scratch,
) {
    // Order both lists by (sub(key), key, time): one counting-sort pass on a hash of the key,
    // then small in-cache sorts. Every person's records end up contiguous, in time order.
    let k = ((evs.len() / 16).max(1).next_power_of_two().trailing_zeros()).clamp(2, 16);
    let sub = |key: u64| -> usize { (fmix(key ^ 0x2545_f491_4f6c_dd1d) >> (64 - k)) as usize };
    let nsub = 1usize << k;
    let Scratch { catsum, touched, sess, ct, ebuf, cbuf, cnt } = sc;
    cnt.clear();
    cnt.resize(nsub + 1, 0);
    for e in evs.iter() {
        cnt[sub(e.key) + 1] += 1;
    }
    for i in 0..nsub {
        cnt[i + 1] += cnt[i];
    }
    ebuf.clear();
    ebuf.resize(evs.len(), Ev { key: 0, meta: 0 });
    {
        let mut pos = cnt.clone();
        for e in evs.iter() {
            let s = sub(e.key);
            ebuf[pos[s]] = *e;
            pos[s] += 1;
        }
    }
    for i in 0..nsub {
        let g = &mut ebuf[cnt[i]..cnt[i + 1]];
        if g.len() > 1 {
            g.sort_unstable_by_key(|e| (e.key, e.meta));
        }
    }
    evs.copy_from_slice(ebuf);
    if !clks.is_empty() {
        cnt.iter_mut().for_each(|c| *c = 0);
        for c in clks.iter() {
            cnt[sub(c.key) + 1] += 1;
        }
        for i in 0..nsub {
            cnt[i + 1] += cnt[i];
        }
        cbuf.clear();
        cbuf.resize(clks.len(), Clk { key: 0, cid: 0, ts: 0, camp: 0 });
        let mut pos = cnt.clone();
        for c in clks.iter() {
            let s = sub(c.key);
            cbuf[pos[s]] = *c;
            pos[s] += 1;
        }
        for i in 0..nsub {
            let g = &mut cbuf[cnt[i]..cnt[i + 1]];
            if g.len() > 1 {
                g.sort_unstable_by(|a, b| (a.key, a.ts).cmp(&(b.key, b.ts)).then_with(|| cmp_ids(a.cid, b.cid, side)));
            }
        }
        clks.copy_from_slice(cbuf);
    }
    let ord = |key: u64| (sub(key), key);
    let mut ci = 0usize;
    let mut i = 0usize;
    let nd = ctx.ndays;
    while i < evs.len() {
        let key = evs[i].key;
        let mut j = i + 1;
        while j < evs.len() && evs[j].key == key {
            j += 1;
        }
        let g = &evs[i..j];
        // sessions and user rows
        sess.clear();
        let mut cur_day: i64 = i64::MIN;
        let (mut ds, mut de, mut dr) = (0i64, 0i64, 0i64);
        let mut any_p = false;
        let mut k = 0;
        let flush = |rows: &mut Vec<UserRow>,
                     day: i64,
                     ds: i64,
                     de: i64,
                     dr: i64,
                     any_p: bool,
                     catsum: &mut Vec<i64>,
                     touched: &mut Vec<u16>| {
            let mut top: Option<u16> = None;
            if any_p {
                touched.sort_unstable();
                let mut best = -1i64;
                for &c in touched.iter() {
                    if catsum[c as usize] > best {
                        best = catsum[c as usize];
                        top = Some(c);
                    }
                }
            }
            for &c in touched.iter() {
                catsum[c as usize] = 0;
            }
            touched.clear();
            rows.push(UserRow { key, day: day as i32, sessions: ds, events: de, revenue: dr, top });
        };
        while k < g.len() {
            let t0 = m_ts(g[k].meta);
            let start = ctx.ws + t0 as i64;
            let day = start.div_euclid(DAY);
            if day != cur_day {
                if cur_day != i64::MIN {
                    flush(rows, cur_day, ds, de, dr, any_p, catsum, touched);
                }
                cur_day = day;
                ds = 0;
                de = 0;
                dr = 0;
                any_p = false;
            }
            let mut last = t0;
            let mut rev = 0i64;
            let mut purch = false;
            let mut n = 0i64;
            while k < g.len() {
                let m = g[k].meta;
                let t = m_ts(m);
                if (t - last) as i64 > GAP {
                    break;
                }
                last = t;
                n += 1;
                if m_typ(m) == 3 {
                    let price = m_price(m);
                    rev += price;
                    purch = true;
                    let c = m_cat(m);
                    if catsum[c] == 0 {
                        touched.push(c as u16);
                    }
                    catsum[c] += price;
                }
                k += 1;
            }
            ds += 1;
            de += n;
            dr += rev;
            any_p |= purch;
            sess.push(Sess { start, rev, purch, d: (day - ctx.day0) as usize });
        }
        flush(rows, cur_day, ds, de, dr, any_p, catsum, touched);

        // this person's clicks
        let okey = ord(key);
        while ci < clks.len() && ord(clks[ci].key) < okey {
            ci += 1;
        }
        let mut cj = ci;
        while cj < clks.len() && clks[cj].key == key {
            cj += 1;
        }
        let c = &clks[ci..cj];
        ci = cj;
        if c.is_empty() {
            for s in sess.iter() {
                let a = &mut acc[ctx.ncamp * nd + s.d];
                a[0] += 1;
                a[1] += s.purch as i64;
                a[2] += s.rev;
            }
            i = j;
            continue;
        }
        // credit: latest click at or before the start, within 7 days; ties -> smallest click_id
        let mut p = 0usize;
        for s in sess.iter() {
            while p < c.len() && ctx.cws + c[p].ts as i64 <= s.start {
                p += 1;
            }
            let mut camp = ctx.ncamp;
            if p > 0 {
                let t = c[p - 1].ts;
                if s.start - (ctx.cws + t as i64) <= WEEK {
                    let mut q = p - 1;
                    while q > 0 && c[q - 1].ts == t {
                        q -= 1;
                    }
                    camp = c[q].camp as usize;
                }
            }
            let a = &mut acc[camp * nd + s.d];
            a[0] += 1;
            a[1] += s.purch as i64;
            a[2] += s.rev;
        }
        // assists: per campaign, the union of [click, click + 7d] covers the assisted starts
        ct.clear();
        ct.extend(c.iter().map(|x| (x.camp, ctx.cws + x.ts as i64)));
        ct.sort_unstable();
        let mut x = 0;
        while x < ct.len() {
            let camp = ct[x].0;
            let mut a = ct[x].1;
            let mut b = a + WEEK;
            x += 1;
            loop {
                let more = x < ct.len() && ct[x].0 == camp;
                if more && ct[x].1 <= b {
                    b = ct[x].1 + WEEK;
                    x += 1;
                    continue;
                }
                let lo = sess.partition_point(|s| s.start < a);
                let hi = sess.partition_point(|s| s.start <= b);
                for s in &sess[lo..hi] {
                    acc[camp as usize * nd + s.d][3] += 1;
                }
                if more {
                    a = ct[x].1;
                    b = a + WEEK;
                    x += 1;
                } else {
                    break;
                }
            }
        }
        i = j;
    }
}

// ---------------------------------------------------------------- parquet

/// Minimal Parquet writer for flat tables: one row group, one PLAIN-encoded uncompressed
/// data page (v1) per column, Thrift compact footer.
mod pq {
    pub const INT32: i32 = 1;
    pub const INT64: i32 = 2;
    pub const BYTE_ARRAY: i32 = 6;
    pub const UTF8: i32 = 0; // converted types
    pub const DATE: i32 = 6;

    pub struct Col {
        pub name: &'static str,
        pub ptype: i32,
        pub conv: Option<i32>,
        pub optional: bool,
        pub data: Vec<u8>,
        defs: Vec<u8>,
        rows: usize,
    }

    impl Col {
        pub fn new(name: &'static str, ptype: i32, conv: Option<i32>, optional: bool, rows_hint: usize) -> Col {
            let per = match ptype {
                INT32 => 4,
                INT64 => 8,
                _ => 16,
            };
            Col { name, ptype, conv, optional, data: Vec::with_capacity(rows_hint * per), defs: Vec::new(), rows: 0 }
        }
        #[inline]
        fn def(&mut self, present: bool) {
            if self.rows % 8 == 0 {
                self.defs.push(0);
            }
            if present {
                *self.defs.last_mut().unwrap() |= 1 << (self.rows % 8);
            }
            self.rows += 1;
        }
        #[inline]
        pub fn i32(&mut self, v: i32) {
            self.data.extend_from_slice(&v.to_le_bytes());
            self.rows += 1;
        }
        #[inline]
        pub fn i64(&mut self, v: i64) {
            self.data.extend_from_slice(&v.to_le_bytes());
            self.rows += 1;
        }
        #[inline]
        pub fn bytes(&mut self, s: &[u8]) {
            self.data.extend_from_slice(&(s.len() as u32).to_le_bytes());
            self.data.extend_from_slice(s);
            if self.optional {
                self.def(true);
            } else {
                self.rows += 1;
            }
        }
        /// Append a string written by `f` into the value buffer (length patched afterwards).
        #[inline]
        pub fn bytes_with(&mut self, f: impl FnOnce(&mut Vec<u8>)) {
            let at = self.data.len();
            self.data.extend_from_slice(&[0; 4]);
            f(&mut self.data);
            let n = (self.data.len() - at - 4) as u32;
            self.data[at..at + 4].copy_from_slice(&n.to_le_bytes());
            self.rows += 1;
        }
        #[inline]
        pub fn null(&mut self) {
            self.def(false);
        }
    }

    struct T {
        b: Vec<u8>,
        last: Vec<i16>,
    }

    impl T {
        fn varint(&mut self, mut v: u64) {
            while v >= 0x80 {
                self.b.push((v as u8) | 0x80);
                v >>= 7;
            }
            self.b.push(v as u8);
        }
        fn field(&mut self, id: i16, ty: u8) {
            let last = self.last.last_mut().unwrap();
            let d = id - *last;
            if d > 0 && d <= 15 {
                self.b.push(((d as u8) << 4) | ty);
            } else {
                self.b.push(ty);
                let z = ((id << 1) ^ (id >> 15)) as u16 as u64;
                self.varint(z);
            }
            *self.last.last_mut().unwrap() = id;
        }
        fn i32f(&mut self, id: i16, v: i32) {
            self.field(id, 5);
            self.varint(((v << 1) ^ (v >> 31)) as u32 as u64);
        }
        fn i64f(&mut self, id: i16, v: i64) {
            self.field(id, 6);
            self.varint(((v << 1) ^ (v >> 63)) as u64);
        }
        fn binf(&mut self, id: i16, s: &[u8]) {
            self.field(id, 8);
            self.varint(s.len() as u64);
            self.b.extend_from_slice(s);
        }
        fn struct_begin(&mut self, id: i16) {
            self.field(id, 12);
            self.last.push(0);
        }
        fn elem_begin(&mut self) {
            self.last.push(0);
        }
        fn struct_end(&mut self) {
            self.b.push(0);
            self.last.pop();
        }
        fn list_begin(&mut self, id: i16, elem: u8, n: usize) {
            self.field(id, 9);
            if n < 15 {
                self.b.push(((n as u8) << 4) | elem);
            } else {
                self.b.push(0xF0 | elem);
                self.varint(n as u64);
            }
        }
    }

    pub fn write(path: &std::path::Path, cols: Vec<Col>) {
        use std::io::Write;
        let rows = cols.first().map(|c| c.rows).unwrap_or(0);
        let mut out: Vec<u8> = Vec::with_capacity(cols.iter().map(|c| c.data.len() + c.defs.len() + 64).sum::<usize>() + 4096);
        out.extend_from_slice(b"PAR1");
        let mut chunks: Vec<(i64, i64)> = Vec::new(); // (offset, size)
        if rows > 0 {
            for c in &cols {
                assert_eq!(c.rows, rows, "column {} has {} rows, expected {}", c.name, c.rows, rows);
                let mut body: Vec<u8> = Vec::new();
                if c.optional {
                    let mut lv = T { b: Vec::new(), last: vec![0] };
                    lv.varint(((c.defs.len() as u64) << 1) | 1); // one bit-packed run
                    lv.b.extend_from_slice(&c.defs);
                    body.extend_from_slice(&(lv.b.len() as u32).to_le_bytes());
                    body.extend_from_slice(&lv.b);
                }
                let size = body.len() + c.data.len();
                let mut h = T { b: Vec::new(), last: vec![0] };
                h.i32f(1, 0); // DATA_PAGE
                h.i32f(2, size as i32);
                h.i32f(3, size as i32);
                h.struct_begin(5);
                h.i32f(1, rows as i32);
                h.i32f(2, 0); // PLAIN
                h.i32f(3, 3); // RLE
                h.i32f(4, 3); // RLE
                h.struct_end();
                h.b.push(0);
                let off = out.len() as i64;
                out.extend_from_slice(&h.b);
                out.extend_from_slice(&body);
                out.extend_from_slice(&c.data);
                chunks.push((off, out.len() as i64 - off));
            }
        }
        // footer: FileMetaData
        let mut t = T { b: Vec::new(), last: vec![0] };
        t.i32f(1, 1);
        t.list_begin(2, 12, cols.len() + 1);
        t.elem_begin();
        t.binf(4, b"schema");
        t.i32f(5, cols.len() as i32);
        t.struct_end();
        for c in &cols {
            t.elem_begin();
            t.i32f(1, c.ptype);
            t.i32f(3, if c.optional { 1 } else { 0 });
            t.binf(4, c.name.as_bytes());
            if let Some(cv) = c.conv {
                t.i32f(6, cv);
                t.struct_begin(10);
                t.struct_begin(if cv == DATE { 6 } else { 1 });
                t.struct_end();
                t.struct_end();
            }
            t.struct_end();
        }
        t.i64f(3, rows as i64);
        if rows > 0 {
            t.list_begin(4, 12, 1);
            t.elem_begin();
            t.list_begin(1, 12, cols.len());
            for (c, &(off, size)) in cols.iter().zip(&chunks) {
                t.elem_begin();
                t.i64f(2, off);
                t.struct_begin(3);
                t.i32f(1, c.ptype);
                t.list_begin(2, 5, 2);
                t.varint(0); // PLAIN (zigzag 0)
                t.varint(6); // RLE (zigzag 3)
                t.list_begin(3, 8, 1);
                t.varint(c.name.len() as u64);
                t.b.extend_from_slice(c.name.as_bytes());
                t.i32f(4, 0); // UNCOMPRESSED
                t.i64f(5, rows as i64);
                t.i64f(6, size);
                t.i64f(7, size);
                t.i64f(9, off);
                t.struct_end();
                t.struct_end();
            }
            t.i64f(2, chunks.iter().map(|c| c.1).sum());
            t.i64f(3, rows as i64);
            t.struct_end();
        } else {
            t.list_begin(4, 12, 0);
        }
        t.binf(6, b"shopping-sessions");
        t.b.push(0);
        out.extend_from_slice(&t.b);
        out.extend_from_slice(&(t.b.len() as u32).to_le_bytes());
        out.extend_from_slice(b"PAR1");
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(&out).unwrap();
    }
}



// ---------------------------------------------------------------- main

fn main() {
    let t0 = Instant::now();
    let args: Vec<String> = std::env::args().collect();
    let data_dir = PathBuf::from(
        std::env::var("DATA_DIR").ok().or_else(|| args.get(1).cloned()).unwrap_or_else(|| "/data".into()),
    );
    let out_dir = PathBuf::from(
        std::env::var("OUTPUT_DIR").ok().or_else(|| args.get(2).cloned()).unwrap_or_else(|| "/output".into()),
    );
    let threads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n: &usize| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8));
    rayon::ThreadPoolBuilder::new().num_threads(threads).build_global().ok();

    let manifest = std::fs::read(data_dir.join("manifest.json")).expect("manifest");
    let ws = manifest_ts(&manifest, "window_start");
    let we = manifest_ts(&manifest, "window_end");
    assert!(we - ws + WEEK < (1i64 << 31), "window too long for 31-bit timestamps");
    let cws = ws - WEEK;
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1).max(1) as usize;

    // products / categories
    let prods = small_csv(&data_dir.join("products.csv"));
    let mut catnames: Vec<S> = prods.iter().map(|p| p.1).collect();
    catnames.push(b"unknown");
    catnames.sort_unstable();
    catnames.dedup();
    assert!(catnames.len() <= 128, "too many categories");
    let mut catidx = Lookup::new();
    for (i, c) in catnames.iter().enumerate() {
        catidx.insert(c, i as u32);
    }
    let unknown = catidx.get(b"unknown").unwrap();
    let prod_ids: Vec<(S, u32)> = prods.iter().map(|&(p, c)| (p, catidx.get(c).unwrap())).collect();
    let prod = IdMap::build(&prod_ids, b"p");
    // campaigns
    let camps = small_csv(&data_dir.join("campaigns.csv"));
    let mut seen = Lookup::new();
    let mut camp_names: Vec<(S, S)> = Vec::new();
    let mut camp_ids: Vec<(S, u32)> = Vec::new();
    for &(c, ch) in &camps {
        if seen.get(c).is_none() {
            seen.insert(c, camp_names.len() as u32);
            camp_ids.push((c, camp_names.len() as u32));
            camp_names.push((c, ch));
        }
    }
    let camp = IdMap::build(&camp_ids, b"c_");
    let ncamp = camp_names.len();
    camp_names.push((b"organic", b"organic"));
    let ctx = Ctx { ws, cws, day0, ndays, ncamp, ncat: catnames.len() };
    log(t0, &format!("setup: {} products, {} campaigns, {} days, {} threads", prods.len(), ncamp, ndays, threads));

    // ---- parse links, events and clicks
    let (pairs, mut side_list) = parse_links(&data_dir.join("links"), threads);
    // the person table is only needed after dedup: build it while events are parsed
    let link_side: Side = side_list.iter().copied().collect();
    let people_thread = std::thread::spawn(move || PersonMap::build(&build_people(pairs, &link_side, threads)));
    log(t0, "links parsed");

    let ech = chunks(&data_dir.join("events"));
    let ebytes: usize = ech.iter().map(|c| c.len()).sum();
    let est_lines = ebytes / 70 + 1;
    let dbits = ((est_lines / 160_000).max(1).next_power_of_two().trailing_zeros()).clamp(6, 11);
    let nd_parts = 1usize << dbits;
    let dshift = 64 - dbits;
    let cap = est_lines / (threads * nd_parts) * 5 / 4 + 16;
    let parsed: Vec<(Vec<Vec<Raw>>, Vec<(u64, S)>)> = run_par(
        ech.len(),
        threads,
        || (parts::<Raw>(nd_parts, cap), Vec::new()),
        |(out, side), ci| {
            scan::<7>(ech[ci], |f| {
                if f[0].is_empty() {
                    return;
                }
                let eid = enc(f[0]);
                let ingest = parse_ts(f[3]).unwrap_or(i64::MAX);
                let mut r = Raw { eid, user: 0, ingest, meta: 0 };
                let t = f[4];
                let typ: u8 = if t.len() == 4 {
                    match u32::from_le_bytes(t.try_into().unwrap()) {
                        0x7765_6976 => 1, // "view"
                        0x7472_6163 => 2, // "cart"
                        _ => 0,
                    }
                } else if t.len() == 8 && u64::from_le_bytes(t.try_into().unwrap()) == u64::from_le_bytes(*b"purchase") {
                    3
                } else {
                    0
                };
                if typ != 0 && !f[1].is_empty() && !f[5].is_empty() {
                    if let Some(ts) = parse_ts(f[2]) {
                        if ts >= ws && ts < we && ingest - ts <= LATE {
                            let price = if typ == 3 { parse_price(f[6]) } else { Some(0) };
                            if let Some(price) = price {
                                let cat = prod.get(f[5]).unwrap_or(unknown);
                                r.meta = meta((ts - ws) as u32, typ, cat, price);
                                r.user = enc(f[1]);
                                if r.user & HBIT != 0 {
                                    side.push((r.user, f[1]));
                                }
                            }
                        }
                    }
                }
                out[(fmix(eid) >> dshift) as usize].push(r);
            });
        },
    );
    log(t0, &format!("events parsed: {} chunks, {} dedup partitions", ech.len(), nd_parts));

    let pmap = people_thread.join().unwrap();
    let pmap = &pmap;
    log(t0, "people built");
    let pbits = ((est_lines / 60_000).max(1).next_power_of_two().trailing_zeros()).clamp(6, 12);
    let np = 1usize << pbits;
    let pshift = 64 - pbits;
    let pb = |key: u64| -> usize { (fmix(key ^ 0x5bd1e995) >> pshift) as usize };

    let cch = chunks(&data_dir.join("clicks"));
    let cbytes: usize = cch.iter().map(|c| c.len()).sum();
    let est_clicks = cbytes / 45 + 1;
    let ccap = est_clicks / (threads * np) * 5 / 4 + 16;
    const RING: usize = 16;
    let cparsed: Vec<(Vec<Vec<Clk>>, Vec<(u64, S)>)> = run_par(
        cch.len(),
        threads,
        || (parts::<Clk>(np, ccap), Vec::new()),
        |(out, side), ci| {
            // look persons up RING clicks after prefetching their slot
            let mut ring = [Clk { key: 0, cid: 0, ts: 0, camp: 0 }; RING];
            let mut n = 0usize;
            scan::<4>(cch[ci], |f| {
                if f[0].is_empty() || f[1].is_empty() {
                    return;
                }
                let ts = match parse_ts(f[3]) {
                    Some(t) if t >= cws && t < we => t,
                    _ => return,
                };
                let cp = match camp.get(f[2]) {
                    Some(c) => c,
                    None => return,
                };
                let user = enc(f[1]);
                let cid = enc(f[0]);
                if user & HBIT != 0 {
                    side.push((user, f[1]));
                }
                if cid & HBIT != 0 {
                    side.push((cid, f[0]));
                }
                pmap.prefetch(user);
                let slot = n % RING;
                if n >= RING {
                    let c = ring[slot];
                    let key = pmap.key(c.key);
                    out[pb(key)].push(Clk { key, ..c });
                }
                ring[slot] = Clk { key: user, cid, ts: (ts - cws) as u32, camp: cp };
                n += 1;
            });
            for i in n.saturating_sub(RING)..n {
                let c = ring[i % RING];
                let key = pmap.key(c.key);
                out[pb(key)].push(Clk { key, ..c });
            }
        },
    );
    log(t0, "clicks parsed");

    // hashed IDs: original text (for names and click_id tie-breaks)
    for (_, s) in &parsed {
        side_list.extend_from_slice(s);
    }
    for (_, s) in &cparsed {
        side_list.extend_from_slice(s);
    }
    let mut side: Side = Side::new();
    for (c, s) in side_list {
        if let Some(&o) = side.get(&c) {
            assert!(o == s, "64-bit hash collision between two IDs");
        } else {
            side.insert(c, s);
        }
    }
    let side = &side;

    // ---- dedup each partition: keep the copy with the smallest ingest_ts, then drop it if invalid
    let ucap = est_lines / (threads * np) * 5 / 4 + 16;
    let survivors: Vec<(Vec<Vec<Ev>>, Vec<u64>, Vec<u64>)> = run_par(
        nd_parts,
        threads,
        || (parts::<Ev>(np, ucap), Vec::new(), Vec::<u64>::new()),
        |(out, table, win), b| {
            let segs: Vec<&[Raw]> = parsed.iter().map(|(p, _)| &p[b][..]).collect();
            let mut offs = Vec::with_capacity(segs.len() + 1);
            let mut n = 0usize;
            for s in &segs {
                offs.push(n);
                n += s.len();
            }
            offs.push(n);
            let get = |g: usize| -> &Raw {
                let mut t = 0;
                while g >= offs[t + 1] {
                    t += 1;
                }
                &segs[t][g - offs[t]]
            };
            let size = (n * 2).next_power_of_two().max(16);
            let mask = size - 1;
            table.clear();
            table.resize(size, 0);
            // slot: (32-bit fingerprint << 32) | (index + 1); 0 = empty.
            // win: bitmap of the current winner of each event_id, so the output pass can read
            // records in order instead of hopping through the table.
            win.clear();
            win.resize(n / 64 + 1, 0);
            let mut g = 0usize;
            for s in &segs {
                for (ri, r) in s.iter().enumerate() {
                    #[cfg(target_arch = "x86_64")]
                    if ri + 16 < s.len() {
                        unsafe {
                            use std::arch::x86_64::*;
                            let ph = (fmix(s[ri + 16].eid) as usize) & mask;
                            _mm_prefetch(table.as_ptr().add(ph) as *const i8, _MM_HINT_T0);
                        }
                    }
                    let h = fmix(r.eid);
                    let fp = (h >> 16) & 0xFFFF_FFFF;
                    let mut pos = (h as usize) & mask;
                    loop {
                        let slot = table[pos];
                        if slot == 0 {
                            table[pos] = (fp << 32) | (g as u64 + 1);
                            win[g >> 6] |= 1 << (g & 63);
                            break;
                        }
                        if slot >> 32 == fp {
                            let j = (slot & 0xFFFF_FFFF) as usize - 1;
                            let o = get(j);
                            if o.eid == r.eid {
                                if r.ingest < o.ingest {
                                    table[pos] = (fp << 32) | (g as u64 + 1);
                                    win[j >> 6] &= !(1 << (j & 63));
                                    win[g >> 6] |= 1 << (g & 63);
                                }
                                break;
                            }
                        }
                        pos = (pos + 1) & mask;
                    }
                    g += 1;
                }
            }
            let mut g = 0usize;
            for s in &segs {
                for (ri, r) in s.iter().enumerate() {
                    if ri + 16 < s.len() {
                        pmap.prefetch(s[ri + 16].user);
                    }
                    if win[g >> 6] >> (g & 63) & 1 != 0 && m_typ(r.meta) != 0 {
                        let key = pmap.key(r.user);
                        out[pb(key)].push(Ev { key, meta: r.meta });
                    }
                    g += 1;
                }
            }
        },
    );
    drop(parsed);
    log(t0, "events deduped");


    // ---- per person bucket: sessions, user rows, credit and assists
    let res: Vec<(Vec<UserRow>, Acc, Vec<Ev>, Vec<Clk>, Scratch)> = run_par(
        np,
        threads,
        || {
            (
                Vec::new(),
                vec![[0i64; 4]; (ncamp + 1) * ndays],
                Vec::new(),
                Vec::new(),
                Scratch { catsum: vec![0; ctx.ncat], touched: Vec::new(), sess: Vec::new(), ct: Vec::new(), ebuf: Vec::new(), cbuf: Vec::new(), cnt: Vec::new() },
            )
        },
        |(rows, acc, evs, cl, sc), b| {
            evs.clear();
            cl.clear();
            for (e, _, _) in &survivors {
                evs.extend_from_slice(&e[b]);
            }
            for (c, _) in &cparsed {
                cl.extend_from_slice(&c[b]);
            }
            process_bucket(&ctx, side, evs, cl, rows, acc, sc);
        },
    );
    drop(survivors);
    drop(cparsed);
    let mut acc = vec![[0i64; 4]; (ncamp + 1) * ndays];
    let mut row_sets: Vec<Vec<UserRow>> = Vec::new();
    for (r, a, _, _, _) in res {
        row_sets.push(r);
        for (x, y) in acc.iter_mut().zip(a.iter()) {
            for k in 0..4 {
                x[k] += y[k];
            }
        }
    }
    log(t0, &format!("sessions done: {} user rows", row_sets.iter().map(|r| r.len()).sum::<usize>()));

    // ---- write outputs
    let ud = out_dir.join("user_daily");
    let cd = out_dir.join("campaign_daily");
    std::fs::create_dir_all(&ud).unwrap();
    std::fs::create_dir_all(&cd).unwrap();
    let catnames = &catnames;
    let camp_names = &camp_names;
    let acc = &acc;
    let ud = &ud;
    let cd = &cd;
    rayon::scope(|s| {
        s.spawn(move |_| {
            use pq::*;
            let mut cr: Vec<(usize, usize, [i64; 4])> = Vec::new();
            for c in 0..=ncamp {
                for d in 0..ndays {
                    let a = acc[c * ndays + d];
                    if a[0] > 0 || a[3] > 0 {
                        cr.push((c, d, a));
                    }
                }
            }
            let n = cr.len();
            let mut cols = vec![
                Col::new("campaign_id", BYTE_ARRAY, Some(UTF8), false, n),
                Col::new("channel", BYTE_ARRAY, Some(UTF8), false, n),
                Col::new("day", INT32, Some(DATE), false, n),
                Col::new("sessions", INT64, None, false, n),
                Col::new("purchasing_sessions", INT64, None, false, n),
                Col::new("revenue_cents", INT64, None, false, n),
                Col::new("assisted_sessions", INT64, None, false, n),
            ];
            for r in &cr {
                cols[0].bytes(camp_names[r.0].0);
                cols[1].bytes(camp_names[r.0].1);
                cols[2].i32((day0 + r.1 as i64) as i32);
                for k in 0..4 {
                    cols[3 + k].i64(r.2[k]);
                }
            }
            pq::write(&cd.join("part-00000.parquet"), cols);
        });
        for (fi, rs) in row_sets.iter().enumerate() {
            s.spawn(move |_| {
                use pq::*;
                let n = rs.len();
                let mut cols = vec![
                    Col::new("user_id", BYTE_ARRAY, Some(UTF8), false, n),
                    Col::new("day", INT32, Some(DATE), false, n),
                    Col::new("sessions", INT64, None, false, n),
                    Col::new("events", INT64, None, false, n),
                    Col::new("revenue_cents", INT64, None, false, n),
                    Col::new("top_category", BYTE_ARRAY, Some(UTF8), true, n),
                ];
                for r in rs {
                    if r.key & HBIT == 0 {
                        cols[0].bytes_with(|b| dec_into(r.key, b));
                    } else {
                        cols[0].bytes(side[&r.key]);
                    }
                    cols[1].i32(r.day);
                    cols[2].i64(r.sessions);
                    cols[3].i64(r.events);
                    cols[4].i64(r.revenue);
                    match r.top {
                        Some(c) => cols[5].bytes(catnames[c as usize]),
                        None => cols[5].null(),
                    }
                }
                pq::write(&ud.join(format!("part-{fi:05}.parquet")), cols);
            });
        }
    });
    log(t0, "done");
    // skip destructors of large structures
    std::process::exit(0);
}

#[cfg(test)]
mod tests {
    use super::*;

    // reference: original an F1/F2/F3 timestamp to epoch ms. None = bad.
    fn parse_ts_ref(b: &[u8]) -> Option<i64> {
        let n = b.len();
        if n == 0 {
            return None;
        }
        if (n == 20 || n == 25) && b[4] == b'-' {
            let y = d2(b, 0)? * 100 + d2(b, 2)?;
            if b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
                return None;
            }
            let mo = d2(b, 5)?;
            let d = d2(b, 8)?;
            let hh = d2(b, 11)?;
            let mi = d2(b, 14)?;
            let ss = d2(b, 17)?;
            if !(1..=12).contains(&mo) || d < 1 || d > dim(y, mo) || hh > 23 || mi > 59 || ss > 59 {
                return None;
            }
            let mut secs = days_from_civil(y, mo, d) * 86400 + hh * 3600 + mi * 60 + ss;
            if n == 20 {
                if b[19] != b'Z' {
                    return None;
                }
            } else {
                let sign = match b[19] {
                    b'+' => 1,
                    b'-' => -1,
                    _ => return None,
                };
                let oh = d2(b, 20)?;
                if b[22] != b':' {
                    return None;
                }
                let om = d2(b, 23)?;
                if oh > 23 || om > 59 {
                    return None;
                }
                secs -= sign * (oh * 3600 + om * 60);
            }
            return Some(secs * 1000);
        }
        // F3: ASCII digits only
        let mut v: u64 = 0;
        for &c in b {
            let d = c.wrapping_sub(b'0');
            if d >= 10 {
                return None;
            }
            v = v.checked_mul(10)?.checked_add(d as u64)?;
        }
        if v > i64::MAX as u64 {
            return None;
        }
        Some(v as i64)
    }
    
    

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn pick<'a>(&mut self, v: &'a [&'a [u8]]) -> &'a [u8] {
            v[(self.next() % v.len() as u64) as usize]
        }
    }

    fn mutate(r: &mut Rng, s: &[u8]) -> Vec<u8> {
        let mut v = s.to_vec();
        let alphabet: &[u8] = b"0123456789-:TZtz+ ,\"\r.e9x";
        for _ in 0..(r.next() % 3) {
            if v.is_empty() {
                break;
            }
            let i = (r.next() % v.len() as u64) as usize;
            match r.next() % 4 {
                0 => v[i] = alphabet[(r.next() % alphabet.len() as u64) as usize],
                1 => {
                    v.remove(i);
                }
                2 => v.insert(i, alphabet[(r.next() % alphabet.len() as u64) as usize]),
                _ => v[i] = v[i].wrapping_add(1),
            }
        }
        v
    }

    #[test]
    fn timestamps_match_reference() {
        let seeds: Vec<&[u8]> = vec![
            b"2026-10-03T23:58:10Z", b"2026-10-03T19:58:10-04:00", b"1791071890000", b"0001791071890000",
            b"2028-02-29T00:00:00Z", b"2027-02-29T00:00:00Z", b"2026-13-01T00:00:00Z", b"2026-10-03T24:00:00Z",
            b"2026-10-03T23:59:60Z", b"2026-10-04T01:30:00+02:00", b"2026-10-04T01:30:00+24:00", b"2026-10-04T01:30:00+05:60",
            b"2026-10-04T01:30:00+0200", b"2026-10-04T01:30:00-00:00", b"2026-10-04T01:30:00+23:59", b"86400000",
            b"123456789012345678901", b"99999999999999999999", b"9223372036854775807", b"9223372036854775808",
            b"2026-10-03 10:00:00Z", b"2026-10-03t10:00:00Z", b"2026-10-03T10:00:00z", b"2026-10-03T10:00:00.000Z",
            b"0000-01-01T00:00:00Z", b"9999-12-31T23:59:59Z", b"1900-02-29T00:00:00Z", b"2000-02-29T00:00:00Z",
            b"", b"-5", b"1.79e12", b"2026-1O-03T10:00:00Z", "２０26-10-03T10:00:00Z".as_bytes(), "١٧٩١٠٧١٨٩٠٠٠٠".as_bytes(),
        ];
        let mut r = Rng(0x9E3779B97F4A7C15);
        for s in &seeds {
            assert_eq!(parse_ts(s), parse_ts_ref(s), "{:?}", String::from_utf8_lossy(s));
        }
        for _ in 0..2_000_000 {
            let seed = r.pick(&seeds);
            let s = mutate(&mut r, seed);
            assert_eq!(parse_ts(&s), parse_ts_ref(&s), "{:?}", String::from_utf8_lossy(&s));
        }
        // every timestamp in the dev data
        for dir in ["data/dev/events", "data/dev/clicks"] {
            for p in csv_files(Path::new(dir)) {
                let data = std::fs::read(&p).unwrap();
                for line in data.split(|&c| c == b'\n') {
                    for f in line.split(|&c| c == b',') {
                        let f = if f.len() >= 2 && f[0] == b'"' { &f[1..f.len() - 1] } else { f };
                        assert_eq!(parse_ts(f), parse_ts_ref(f), "{:?}", String::from_utf8_lossy(f));
                    }
                }
            }
        }
    }

    fn scan_ref<const N: usize>(chunk: S) -> Vec<Vec<Vec<u8>>> {
        let mut v = Vec::new();
        let mut f: [S; N] = [&[]; N];
        for_each_line(chunk, |line| {
            if fields(line, &mut f) {
                v.push(f.iter().map(|x| x.to_vec()).collect());
            }
        });
        v
    }

    fn scan_new<const N: usize>(chunk: S) -> Vec<Vec<Vec<u8>>> {
        let mut v = Vec::new();
        scan::<N>(chunk, |f| v.push(f.iter().map(|x| x.to_vec()).collect()));
        v
    }

    #[test]
    fn scanner_matches_reference() {
        let leak = |v: Vec<u8>| -> S { Box::leak(v.into_boxed_slice()) };
        // real data
        for p in csv_files(Path::new("data/dev/events")) {
            let d = leak(std::fs::read(&p).unwrap());
            assert!(scan_new::<7>(d) == scan_ref::<7>(d), "{}", p.display());
        }
        for p in csv_files(Path::new("data/dev/links")) {
            let d = leak(std::fs::read(&p).unwrap());
            assert!(scan_new::<2>(d) == scan_ref::<2>(d), "{}", p.display());
        }
        // random mutated blocks of lines, with odd lengths and no trailing newline
        let base = std::fs::read(csv_files(Path::new("data/dev/events"))[0].clone()).unwrap();
        let lines: Vec<&[u8]> = base.split(|&c| c == b'\n').take(3000).collect();
        let mut r = Rng(12345);
        for round in 0..3000 {
            let mut buf = Vec::new();
            let n = (r.next() % 40) as usize;
            for _ in 0..n {
                let l: &[u8] = r.pick(&lines);
                let l = if r.next() % 3 == 0 { mutate(&mut r, l) } else { l.to_vec() };
                buf.extend_from_slice(&l);
                if r.next() % 17 == 0 {
                    buf.extend_from_slice(b"\"\"");
                }
                buf.push(b'\n');
                if r.next() % 23 == 0 {
                    buf.push(b'\n');
                }
            }
            if round % 2 == 0 {
                buf.pop();
            }
            let d = leak(buf);
            assert!(scan_new::<7>(d) == scan_ref::<7>(d), "round {round}");
            assert!(scan_new::<4>(d) == scan_ref::<4>(d), "round {round}");
            assert!(scan_new::<2>(d) == scan_ref::<2>(d), "round {round}");
        }
    }
}
