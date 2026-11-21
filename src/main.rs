//! Shopping Sessions pipeline: implements rules.md exactly.
//! Reads /data CSVs, writes /output/user_daily and /output/campaign_daily as Parquet.

use hashbrown::HashTable;
use memchr::{memchr, memchr_iter};
use rayon::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::Instant;

type S = &'static [u8];

const DAY: i64 = 86_400_000;
const GAP: i64 = 1_800_000;
const WEEK: i64 = 7 * DAY;
const LATE: i64 = DAY;
const LINK_BIT: u64 = 1 << 63;
const CHUNK: usize = 4 << 20;
const DEDUP_BUCKETS: usize = 256;
const PERSON_BUCKETS: usize = 512;
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

/// Exact 16-byte form of an ID: big-endian bytes plus the length in the last byte when
/// the ID is at most 15 bytes long, otherwise a 120-bit hash with 0xFF in the last byte.
type P = (u64, u64);

#[inline]
fn pack(b: &[u8]) -> P {
    if b.len() <= 15 {
        let mut a = [0u8; 16];
        a[..b.len()].copy_from_slice(b);
        a[15] = b.len() as u8;
        (u64::from_be_bytes(a[..8].try_into().unwrap()), u64::from_be_bytes(a[8..].try_into().unwrap()))
    } else {
        use std::hash::Hasher;
        let mut h = rustc_hash::FxHasher::default();
        h.write(&[0x5a, 0xa5]);
        h.write(b);
        let h2 = fmix(h.finish() ^ 0x2545f4914f6cdd1d);
        (hash(b), (h2 << 8) | 0xFF)
    }
}

#[inline]
fn is_short(p: P) -> bool {
    (p.1 & 0xFF) <= 15
}

fn unpack(p: P) -> Vec<u8> {
    let mut a = [0u8; 16];
    a[..8].copy_from_slice(&p.0.to_be_bytes());
    a[8..].copy_from_slice(&p.1.to_be_bytes());
    a[..a[15] as usize].to_vec()
}

#[inline]
fn phash(p: P) -> u64 {
    fmix(p.0 ^ fmix(p.1 ^ 0x9e3779b97f4a7c15))
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
    #[cfg(target_arch = "x86_64")]
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
#[inline(always)]
fn scan<const N: usize>(chunk: S, mut f: impl FnMut(&[S; N])) {
    let len = chunk.len();
    let mut ends = [0usize; N]; // end offsets of fields 0..N-1 (commas)
    let mut nc = 0usize; // commas seen in this line
    let mut nq = 0u32; // quotes seen in this line
    let mut start = 0usize; // line start
    let mut out: [S; N] = [&[]; N];
    // A line is well formed when it has N fields and its only quotes are pairs wrapping whole
    // fields: then the quote count is exactly 2 per wrapped field.
    let mut emit = |start: usize, end: usize, nc: usize, nq: u32, ends: &[usize; N]| {
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
        let mut m = mn | mc | mq;
        while m != 0 {
            let i = m.trailing_zeros() as usize;
            m &= m - 1;
            let pos = base + i;
            let bit = 1u64 << i;
            if mc & bit != 0 {
                if nc < N {
                    ends[nc.min(N - 1)] = pos;
                }
                nc += 1;
            } else if mn & bit != 0 {
                emit(start, pos, nc, nq, &ends);
                start = pos + 1;
                nc = 0;
                nq = 0;
            } else {
                nq += 1;
            }
        }
        base += 64;
    }
    if start < len {
        emit(start, len, nc, nq, &ends);
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
fn leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

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

/// Parse an F1/F2/F3 timestamp to epoch ms. None = bad.
#[inline]
fn parse_ts(b: &[u8]) -> Option<i64> {
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

// ---------------------------------------------------------------- linked IDs

struct People {
    shards: Vec<HashTable<(P, u64)>>, // packed id -> person key
    names: Vec<S>,                       // by root id: smallest member id
}

impl People {
    /// Person key for a user ID: LINK_BIT | root for linked IDs, a hash of the ID otherwise.
    #[inline]
    fn key(&self, user: S) -> u64 {
        let p = pack(user);
        self.key_packed(p, phash(p))
    }
    #[inline]
    fn key_packed(&self, p: P, h: u64) -> u64 {
        match self.shards[(h >> 56) as usize].find(h, |e| e.0 == p) {
            Some(e) => e.1,
            None => h & !LINK_BIT,
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

fn build_people(dir: &Path, t0: Instant) -> People {
    let ch = chunks(dir);
    // parse
    let pairs: Vec<Vec<(u64, S, u64, S, P, P)>> = ch
        .par_iter()
        .map(|&c| {
            let mut v = Vec::new();
            scan::<2>(c, |f| {
                if !f[0].is_empty() && !f[1].is_empty() {
                    let (pa, pb) = (pack(f[0]), pack(f[1]));
                    v.push((phash(pa), f[0], phash(pb), f[1], pa, pb));
                }
            });
            v
        })
        .collect();
    // shard endpoints
    let per_shard: Vec<Vec<Vec<(u64, S, P)>>> = pairs
        .par_iter()
        .map(|v| {
            let mut s: Vec<Vec<(u64, S, P)>> = vec![Vec::new(); SHARDS];
            for &(ha, a, hb, b, pa, pb) in v {
                s[(ha >> 56) as usize].push((ha, a, pa));
                s[(hb >> 56) as usize].push((hb, b, pb));
            }
            s
        })
        .collect();
    let mut shard_tables: Vec<(HashTable<(P, u64)>, Vec<S>)> = (0..SHARDS)
        .into_par_iter()
        .map(|sh| {
            let mut t: HashTable<(P, u64)> = HashTable::new();
            let mut ids: Vec<S> = Vec::new();
            for part in &per_shard {
                for &(h, k, p) in &part[sh] {
                    if t.find(h, |e| e.0 == p).is_none() {
                        t.insert_unique(h, (p, ids.len() as u64), |e| phash(e.0));
                        ids.push(k);
                    }
                }
            }
            (t, ids)
        })
        .collect();
    drop(per_shard);
    // global ids
    let mut offs = vec![0u32; SHARDS];
    let mut total = 0u32;
    for (i, (_, ids)) in shard_tables.iter().enumerate() {
        offs[i] = total;
        total += ids.len() as u32;
    }
    let mut all_ids: Vec<S> = Vec::with_capacity(total as usize);
    for (_, ids) in &shard_tables {
        all_ids.extend_from_slice(ids);
    }
    shard_tables.par_iter_mut().enumerate().for_each(|(i, (t, _))| {
        let o = offs[i] as u64;
        for e in t.iter_mut() {
            e.1 += o;
        }
    });
    let mut shards: Vec<HashTable<(P, u64)>> = shard_tables.into_iter().map(|(t, _)| t).collect();
    let gid = |h: u64, p: P| -> u32 { shards[(h >> 56) as usize].find(h, |e| e.0 == p).unwrap().1 as u32 };
    let edges: Vec<Vec<(u32, u32)>> = pairs
        .par_iter()
        .map(|v| v.iter().map(|&(ha, _, hb, _, pa, pb)| (gid(ha, pa), gid(hb, pb))).collect())
        .collect();
    drop(pairs);
    let mut parent: Vec<u32> = (0..total).collect();
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
    for i in 0..total {
        root[i as usize] = find(&mut parent, i);
    }
    let mut names: Vec<S> = all_ids.clone();
    for i in 0..total as usize {
        let r = root[i] as usize;
        if all_ids[i] < names[r] {
            names[r] = all_ids[i];
        }
    }
    let root = &root;
    shards.par_iter_mut().for_each(|t| {
        for e in t.iter_mut() {
            e.1 = LINK_BIT | root[e.1 as usize] as u64;
        }
    });
    log(t0, &format!("links: {} ids, {} edges", total, edges.iter().map(|v| v.len()).sum::<usize>()));
    People { shards, names }
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
    if s.len() <= prefix.len() || s.len() > prefix.len() + 7 || &s[..prefix.len()] != prefix {
        return None;
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

/// Event payload in one word: ts (ms since window_start, 31 bits) | typ (2) | cat (7) | price (24).
#[inline]
fn meta(ts: u32, typ: u8, cat: u32, price: u32) -> u64 {
    ts as u64 | (typ as u64) << 31 | (cat as u64) << 33 | (price as u64) << 40
}
#[inline]
fn m_ts(m: u64) -> u32 {
    (m & 0x7FFF_FFFF) as u32
}
#[inline]
fn m_typ(m: u64) -> u8 {
    ((m >> 31) & 3) as u8
}
#[inline]
fn m_cat(m: u64) -> usize {
    ((m >> 33) & 0x7F) as usize
}
#[inline]
fn m_price(m: u64) -> i64 {
    (m >> 40) as i64
}

/// An event row as parsed, kept for dedup. typ 0 in meta = the row is invalid.
#[derive(Clone, Copy)]
struct Raw {
    eid: P,
    up: P,
    ingest: i64,
    meta: u64,
}

/// A surviving event, before its person is known.
#[derive(Clone, Copy)]
struct EvU {
    up: P,
    meta: u64,
}

#[derive(Clone, Copy)]
struct Ev {
    key: u64,
    meta: u64,
    up: P,
}

#[derive(Clone, Copy)]
struct ClkU {
    up: P,
    ts: i64,
    cid: S,
    camp: u32,
}

#[derive(Clone, Copy)]
struct Clk {
    key: u64,
    ts: i64,
    cid: S,
    camp: u32,
}

enum Name {
    Str(S),
    Packed(P),
}

struct UserRow {
    name: Name,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: Option<u16>,
}

struct Ctx {
    ws: i64,
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
}

fn process_bucket(
    ctx: &Ctx,
    people: &People,
    long_names: &hashbrown::HashMap<P, S>,
    evs: &mut [Ev],
    clks: &mut [Clk],
    rows: &mut Vec<UserRow>,
    acc: &mut Acc,
    sc: &mut Scratch,
) {
    evs.sort_unstable_by_key(|e| (e.key, m_ts(e.meta)));
    clks.sort_unstable_by(|a, b| (a.key, a.ts, a.cid).cmp(&(b.key, b.ts, b.cid)));
    let Scratch { catsum, touched, sess, ct } = sc;
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
        let up = g[0].up;
        let name = || -> Name {
            if key & LINK_BIT != 0 {
                Name::Str(people.names[(key & !LINK_BIT) as usize])
            } else if is_short(up) {
                Name::Packed(up)
            } else {
                Name::Str(long_names[&up])
            }
        };
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
            rows.push(UserRow { name: name(), day: day as i32, sessions: ds, events: de, revenue: dr, top });
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
        while ci < clks.len() && clks[ci].key < key {
            ci += 1;
        }
        let mut cj = ci;
        while cj < clks.len() && clks[cj].key == key {
            cj += 1;
        }
        let c = &clks[ci..cj];
        ci = cj;
        // credit: latest click at or before the start, within 7 days; ties -> smallest click_id
        let mut p = 0usize;
        for s in sess.iter() {
            while p < c.len() && c[p].ts <= s.start {
                p += 1;
            }
            let mut camp = ctx.ncamp;
            if p > 0 {
                let t = c[p - 1].ts;
                if s.start - t <= WEEK {
                    let mut q = p - 1;
                    while q > 0 && c[q - 1].ts == t {
                        q -= 1;
                    }
                    camp = c[q].camp as usize;
                }
            }
            let a = &mut acc[camp * nd + s.d];
            a[0] += 1;
            if s.purch {
                a[1] += 1;
            }
            a[2] += s.rev;
        }
        // assists: per campaign, the union of [click, click + 7d] covers the assisted starts
        if !c.is_empty() {
            ct.clear();
            ct.extend(c.iter().map(|x| (x.camp, x.ts)));
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
        }
        i = j;
    }
}

// ---------------------------------------------------------------- parquet

mod pq {
    use super::*;
    use parquet::basic::Compression;
    use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;

    pub enum Col {
        Str(Vec<ByteArray>, Option<Vec<i16>>),
        I32(Vec<i32>),
        I64(Vec<i64>),
    }

    pub fn ba(s: S) -> ByteArray {
        ByteArray::from(bytes::Bytes::from_static(s))
    }

    pub fn write(path: &Path, schema: &str, cols: Vec<Col>) {
        let schema = Arc::new(parse_message_type(schema).unwrap());
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::UNCOMPRESSED)
                .set_dictionary_enabled(false)
                .set_statistics_enabled(EnabledStatistics::None)
                .set_max_row_group_size(usize::MAX)
                .build(),
        );
        let file = std::fs::File::create(path).unwrap();
        let file = std::io::BufWriter::with_capacity(1 << 20, file);
        let mut w = SerializedFileWriter::new(file, schema, props).unwrap();
        let mut rg = w.next_row_group().unwrap();
        let mut it = cols.into_iter();
        while let Some(mut col) = rg.next_column().unwrap() {
            match it.next().unwrap() {
                Col::Str(v, def) => {
                    col.typed::<ByteArrayType>().write_batch(&v, def.as_deref(), None).unwrap();
                }
                Col::I32(v) => {
                    col.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
                }
                Col::I64(v) => {
                    col.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
                }
            }
            col.close().unwrap();
        }
        rg.close().unwrap();
        w.close().unwrap();
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
    assert!(we - ws < (1i64 << 31), "window too long for 31-bit timestamps");
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
    let ctx = Ctx { ws, day0, ndays, ncamp, ncat: catnames.len() };
    log(t0, &format!("setup: {} products, {} campaigns, {} days, {} threads", prods.len(), ncamp, ndays, threads));

    let people = build_people(&data_dir.join("links"), t0);

    // ---- events: parse into per-thread partitions by event_id hash
    let ech = chunks(&data_dir.join("events"));
    let ebytes: usize = ech.iter().map(|c| c.len()).sum();
    let est_lines = ebytes / 70 + 1;
    let dbits = ((est_lines / 40_000).max(1).next_power_of_two().trailing_zeros()).clamp(6, 12);
    let nd_parts = 1usize << dbits;
    let dshift = 64 - dbits;
    let cap = est_lines / (threads * nd_parts) * 5 / 4 + 16;
    let parsed: Vec<(Vec<Vec<Raw>>, Vec<(P, S)>)> = run_par(
        ech.len(),
        threads,
        || (parts::<Raw>(nd_parts, cap), Vec::new()),
        |(out, long), ci| {
            scan::<7>(ech[ci], |f| {
                if f[0].is_empty() {
                    return;
                }
                let eid = pack(f[0]);
                let ingest = parse_ts(f[3]).unwrap_or(i64::MAX);
                let mut r = Raw { eid, up: (0, 0), ingest, meta: 0 };
                let typ: u8 = match f[4] {
                    b"view" => 1,
                    b"cart" => 2,
                    b"purchase" => 3,
                    _ => 0,
                };
                if typ != 0 && !f[1].is_empty() && !f[5].is_empty() {
                    if let Some(ts) = parse_ts(f[2]) {
                        if ts >= ws && ts < we && ingest - ts <= LATE {
                            let price = if typ == 3 { parse_price(f[6]) } else { Some(0) };
                            if let Some(price) = price {
                                let cat = prod.get(f[5]).unwrap_or(unknown);
                                r.meta = meta((ts - ws) as u32, typ, cat, price);
                                r.up = pack(f[1]);
                                if !is_short(r.up) {
                                    long.push((r.up, f[1]));
                                }
                            }
                        }
                    }
                }
                out[(phash(eid) >> dshift) as usize].push(r);
            });
        },
    );
    let mut long_names: hashbrown::HashMap<P, S> = hashbrown::HashMap::new();
    for (_, l) in &parsed {
        long_names.extend(l.iter().copied());
    }
    log(t0, &format!("events parsed: {} chunks, {} dedup partitions", ech.len(), nd_parts));

    // ---- dedup each partition: keep the copy with the smallest ingest_ts, then drop it if invalid
    let ucap = est_lines / (threads * SHARDS) * 5 / 4 + 16;
    let survivors: Vec<(Vec<Vec<EvU>>, Vec<u64>)> = run_par(
        nd_parts,
        threads,
        || (parts::<EvU>(SHARDS, ucap), Vec::new()),
        |(out, table), b| {
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
            // slot: (32-bit fingerprint << 32) | (index + 1); 0 = empty
            let mut g = 0usize;
            for s in &segs {
                for r in s.iter() {
                    let h = phash(r.eid);
                    let fp = (h >> 16) & 0xFFFF_FFFF;
                    let mut pos = (h as usize) & mask;
                    loop {
                        let slot = table[pos];
                        if slot == 0 {
                            table[pos] = (fp << 32) | (g as u64 + 1);
                            break;
                        }
                        if slot >> 32 == fp {
                            let j = (slot & 0xFFFF_FFFF) as usize - 1;
                            let o = get(j);
                            if o.eid == r.eid {
                                if r.ingest < o.ingest {
                                    table[pos] = (fp << 32) | (g as u64 + 1);
                                }
                                break;
                            }
                        }
                        pos = (pos + 1) & mask;
                    }
                    g += 1;
                }
            }
            for &slot in table.iter() {
                if slot == 0 {
                    continue;
                }
                let r = get((slot & 0xFFFF_FFFF) as usize - 1);
                if m_typ(r.meta) == 0 {
                    continue;
                }
                out[(phash(r.up) >> 56) as usize].push(EvU { up: r.up, meta: r.meta });
            }
        },
    );
    drop(parsed);
    log(t0, "events deduped");

    // ---- resolve persons one ID shard at a time (each shard's table stays in cache)
    let pbits = ((est_lines / 60_000).max(1).next_power_of_two().trailing_zeros()).clamp(6, 12);
    let np = 1usize << pbits;
    let pshift = 64 - pbits;
    let pb = |key: u64| -> usize { (fmix(key ^ 0x5bd1e995) >> pshift) as usize };
    let pcap = est_lines / (threads * np) * 5 / 4 + 16;
    let events: Vec<Vec<Vec<Ev>>> = run_par(
        SHARDS,
        threads,
        || parts::<Ev>(np, pcap),
        |out, sh| {
            for (part, _) in &survivors {
                for e in &part[sh] {
                    let key = people.key_packed(e.up, phash(e.up));
                    out[pb(key)].push(Ev { key, meta: e.meta, up: e.up });
                }
            }
        },
    );
    drop(survivors);
    log(t0, "event persons resolved");

    // ---- clicks (copies are identical, so duplicates never change credit or assists)
    let cch = chunks(&data_dir.join("clicks"));
    let cbytes: usize = cch.iter().map(|c| c.len()).sum();
    let est_clicks = cbytes / 45 + 1;
    let ccap = est_clicks / (threads * SHARDS) * 5 / 4 + 16;
    let cparsed: Vec<Vec<Vec<ClkU>>> = run_par(
        cch.len(),
        threads,
        || parts::<ClkU>(SHARDS, ccap),
        |out, ci| {
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
                let up = pack(f[1]);
                out[(phash(up) >> 56) as usize].push(ClkU { up, ts, cid: f[0], camp: cp });
            });
        },
    );
    let pccap = est_clicks / (threads * np) * 5 / 4 + 16;
    let clicks: Vec<Vec<Vec<Clk>>> = run_par(
        SHARDS,
        threads,
        || parts::<Clk>(np, pccap),
        |out, sh| {
            for part in &cparsed {
                for c in &part[sh] {
                    let key = people.key_packed(c.up, phash(c.up));
                    out[pb(key)].push(Clk { key, ts: c.ts, cid: c.cid, camp: c.camp });
                }
            }
        },
    );
    drop(cparsed);
    log(t0, "clicks parsed");

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
                Scratch { catsum: vec![0; ctx.ncat], touched: Vec::new(), sess: Vec::new(), ct: Vec::new() },
            )
        },
        |(rows, acc, evs, cl, sc), b| {
            evs.clear();
            for part in &events {
                evs.extend_from_slice(&part[b]);
            }
            cl.clear();
            for part in &clicks {
                cl.extend_from_slice(&part[b]);
            }
            process_bucket(&ctx, &people, &long_names, evs, cl, rows, acc, sc);
        },
    );
    drop(events);
    drop(clicks);
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
    const USCHEMA: &str = "message user_daily { REQUIRED BYTE_ARRAY user_id (UTF8); REQUIRED INT32 day (DATE); \
        REQUIRED INT64 sessions; REQUIRED INT64 events; REQUIRED INT64 revenue_cents; OPTIONAL BYTE_ARRAY top_category (UTF8); }";
    const CSCHEMA: &str = "message campaign_daily { REQUIRED BYTE_ARRAY campaign_id (UTF8); REQUIRED BYTE_ARRAY channel (UTF8); \
        REQUIRED INT32 day (DATE); REQUIRED INT64 sessions; REQUIRED INT64 purchasing_sessions; REQUIRED INT64 revenue_cents; \
        REQUIRED INT64 assisted_sessions; }";

    let catnames = &catnames;
    let camp_names = &camp_names;
    let acc = &acc;
    let ud = &ud;
    let cd = &cd;
    rayon::scope(|s| {
        s.spawn(move |_| {
            let mut cr: Vec<(usize, usize, [i64; 4])> = Vec::new();
            for c in 0..=ncamp {
                for d in 0..ndays {
                    let a = acc[c * ndays + d];
                    if a[0] > 0 || a[3] > 0 {
                        cr.push((c, d, a));
                    }
                }
            }
            use pq::Col;
            let cols = vec![
                Col::Str(cr.iter().map(|r| pq::ba(camp_names[r.0].0)).collect(), None),
                Col::Str(cr.iter().map(|r| pq::ba(camp_names[r.0].1)).collect(), None),
                Col::I32(cr.iter().map(|r| (day0 + r.1 as i64) as i32).collect()),
                Col::I64(cr.iter().map(|r| r.2[0]).collect()),
                Col::I64(cr.iter().map(|r| r.2[1]).collect()),
                Col::I64(cr.iter().map(|r| r.2[2]).collect()),
                Col::I64(cr.iter().map(|r| r.2[3]).collect()),
            ];
            pq::write(&cd.join("part-00000.parquet"), CSCHEMA, cols);
        });
        for (fi, rs) in row_sets.iter().enumerate() {
            s.spawn(move |_| {
                use pq::Col;
                let cols = vec![
                    Col::Str(
                        rs.iter()
                            .map(|r| match r.name {
                                Name::Str(s) => pq::ba(s),
                                Name::Packed(p) => parquet::data_type::ByteArray::from(unpack(p)),
                            })
                            .collect(),
                        None,
                    ),
                    Col::I32(rs.iter().map(|r| r.day).collect()),
                    Col::I64(rs.iter().map(|r| r.sessions).collect()),
                    Col::I64(rs.iter().map(|r| r.events).collect()),
                    Col::I64(rs.iter().map(|r| r.revenue).collect()),
                    Col::Str(
                        rs.iter().filter_map(|r| r.top.map(|c| pq::ba(catnames[c as usize]))).collect(),
                        Some(rs.iter().map(|r| r.top.is_some() as i16).collect()),
                    ),
                ];
                pq::write(&ud.join(format!("part-{fi:05}.parquet")), USCHEMA, cols);
            });
        }
    });
    log(t0, "done");
    // skip destructors of large structures
    std::process::exit(0);
}
