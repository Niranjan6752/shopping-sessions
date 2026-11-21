//! Shopping Sessions pipeline: implements rules.md exactly.
//! Reads /data CSVs, writes /output/user_daily and /output/campaign_daily as Parquet.

use hashbrown::HashTable;
use memchr::{memchr, memchr_iter};
use rayon::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::Arc;
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
    let mut f: [S; 2] = [&[]; 2];
    for_each_line(&data[start..], |line| {
        if fields(line, &mut f) && !f[0].is_empty() {
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
            let mut f: [S; 2] = [&[]; 2];
            for_each_line(c, |line| {
                if fields(line, &mut f) && !f[0].is_empty() && !f[1].is_empty() {
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

// ---------------------------------------------------------------- records

/// An event row as parsed, kept for dedup. typ 0 = invalid kept copy.
#[derive(Clone, Copy)]
struct Raw {
    eid: P,
    up: P,
    ingest: i64,
    ts: u32, // ms since window_start
    price: u32,
    cat: u16,
    typ: u8, // 1 view, 2 cart, 3 purchase
}

/// A surviving event, before its person is known.
#[derive(Clone, Copy)]
struct EvU {
    up: P,
    ts: u32,
    price: u32,
    cat: u16,
    typ: u8,
}

#[derive(Clone, Copy)]
struct Ev {
    key: u64,
    up: P,
    ts: u32,
    price: u32,
    cat: u16,
    typ: u8,
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

#[inline]
fn pbucket(key: u64) -> usize {
    (fmix(key ^ 0x5bd1e995) % PERSON_BUCKETS as u64) as usize
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

fn process_bucket(
    ctx: &Ctx,
    people: &People,
    long_names: &hashbrown::HashMap<P, S>,
    evs: &mut [Ev],
    clks: &mut [Clk],
    rows: &mut Vec<UserRow>,
    acc: &mut Acc,
) {
    evs.sort_unstable_by_key(|e| (e.key, e.ts));
    clks.sort_unstable_by(|a, b| (a.key, a.ts, a.cid).cmp(&(b.key, b.ts, b.cid)));
    let mut catsum = vec![0i64; ctx.ncat];
    let mut touched: Vec<u16> = Vec::new();
    let mut sess: Vec<Sess> = Vec::new();
    let mut ct: Vec<(u32, i64)> = Vec::new();
    let mut ci = 0usize;
    let mut i = 0usize;
    let nd = ctx.ndays;
    while i < evs.len() {
        let key = evs[i].key;
        let mut j = i;
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
        // sessions + user rows
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
            let start = ctx.ws + g[k].ts as i64;
            let day = start.div_euclid(DAY);
            if day != cur_day {
                if cur_day != i64::MIN {
                    flush(rows, cur_day, ds, de, dr, any_p, &mut catsum, &mut touched);
                }
                cur_day = day;
                ds = 0;
                de = 0;
                dr = 0;
                any_p = false;
            }
            let mut last = g[k].ts;
            let mut rev = 0i64;
            let mut purch = false;
            let mut n = 0i64;
            while k < g.len() && (g[k].ts - last) as i64 <= GAP {
                let e = &g[k];
                last = e.ts;
                n += 1;
                if e.typ == 3 {
                    rev += e.price as i64;
                    purch = true;
                    let c = e.cat as usize;
                    if catsum[c] == 0 {
                        touched.push(c as u16);
                    }
                    catsum[c] += e.price as i64;
                }
                k += 1;
            }
            ds += 1;
            de += n;
            dr += rev;
            any_p |= purch;
            sess.push(Sess { start, rev, purch, d: (day - ctx.day0) as usize });
        }
        flush(rows, cur_day, ds, de, dr, any_p, &mut catsum, &mut touched);

        // clicks for this person
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
        for s in &sess {
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
    let threads: usize = std::env::var("NCPU").ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    rayon::ThreadPoolBuilder::new().num_threads(threads).build_global().ok();

    let manifest = std::fs::read(data_dir.join("manifest.json")).expect("manifest");
    let ws = manifest_ts(&manifest, "window_start");
    let we = manifest_ts(&manifest, "window_end");
    assert!(we - ws < u32::MAX as i64, "window too long");
    let cws = ws - WEEK;
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1).max(1) as usize;

    // products / categories
    let prods = small_csv(&data_dir.join("products.csv"));
    let mut catnames: Vec<S> = prods.iter().map(|p| p.1).collect();
    catnames.push(b"unknown");
    catnames.sort_unstable();
    catnames.dedup();
    let mut catidx = Lookup::new();
    for (i, c) in catnames.iter().enumerate() {
        catidx.insert(c, i as u32);
    }
    let unknown = catidx.get(b"unknown").unwrap() as u16;
    let mut prod = Lookup::new();
    for (p, c) in &prods {
        prod.insert(p, catidx.get(c).unwrap());
    }
    // campaigns
    let camps = small_csv(&data_dir.join("campaigns.csv"));
    let mut camp = Lookup::new();
    let mut camp_names: Vec<(S, S)> = Vec::new();
    for (c, ch) in &camps {
        if camp.get(c).is_none() {
            camp.insert(c, camp_names.len() as u32);
            camp_names.push((c, ch));
        }
    }
    let ncamp = camp_names.len();
    camp_names.push((b"organic", b"organic"));
    let ctx = Ctx { ws, day0, ndays, ncamp, ncat: catnames.len() };
    log(t0, &format!("setup: {} products, {} campaigns, {} days", prods.len(), ncamp, ndays));

    let people = build_people(&data_dir.join("links"), t0);

    // ---- events: parse into dedup buckets (by event_id)
    let ech = chunks(&data_dir.join("events"));
    let parsed: Vec<(Vec<Vec<Raw>>, Vec<(P, S)>)> = ech
        .par_iter()
        .map(|&c| {
            let mut out: Vec<Vec<Raw>> = (0..DEDUP_BUCKETS).map(|_| Vec::new()).collect();
            let mut long: Vec<(P, S)> = Vec::new();
            let mut f: [S; 7] = [&[]; 7];
            for_each_line(c, |line| {
                if !fields(line, &mut f) || f[0].is_empty() {
                    return;
                }
                let eid = pack(f[0]);
                let ingest = parse_ts(f[3]).unwrap_or(i64::MAX);
                let mut r = Raw { eid, up: (0, 0), ingest, ts: 0, price: 0, cat: 0, typ: 0 };
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
                                r.ts = (ts - ws) as u32;
                                r.price = price;
                                r.cat = prod.get(f[5]).map(|x| x as u16).unwrap_or(unknown);
                                r.typ = typ;
                                r.up = pack(f[1]);
                                if !is_short(r.up) {
                                    long.push((r.up, f[1]));
                                }
                            }
                        }
                    }
                }
                out[(phash(eid) >> 56) as usize].push(r);
            });
            (out, long)
        })
        .collect();
    let mut long_names: hashbrown::HashMap<P, S> = hashbrown::HashMap::new();
    for (_, l) in &parsed {
        long_names.extend(l.iter().copied());
    }
    log(t0, &format!("events parsed: {} chunks", ech.len()));

    // ---- dedup per bucket: keep the copy with the smallest ingest_ts, then drop invalid kept copies
    let survivors: Vec<Vec<Vec<EvU>>> = (0..DEDUP_BUCKETS)
        .into_par_iter()
        .map(|b| {
            let n: usize = parsed.iter().map(|c| c.0[b].len()).sum();
            let mut t: HashTable<(P, i64, u32, u32)> = HashTable::with_capacity(n);
            for (ci, c) in parsed.iter().enumerate() {
                for (ri, r) in c.0[b].iter().enumerate() {
                    let h = phash(r.eid);
                    match t.find_mut(h, |e| e.0 == r.eid) {
                        Some(e) => {
                            if r.ingest < e.1 {
                                e.1 = r.ingest;
                                e.2 = ci as u32;
                                e.3 = ri as u32;
                            }
                        }
                        None => {
                            t.insert_unique(h, (r.eid, r.ingest, ci as u32, ri as u32), |e| phash(e.0));
                        }
                    }
                }
            }
            let mut out: Vec<Vec<EvU>> = (0..SHARDS).map(|_| Vec::new()).collect();
            for e in t.iter() {
                let r = &parsed[e.2 as usize].0[b][e.3 as usize];
                if r.typ == 0 {
                    continue;
                }
                out[(phash(r.up) >> 56) as usize].push(EvU { up: r.up, ts: r.ts, price: r.price, cat: r.cat, typ: r.typ });
            }
            out
        })
        .collect();
    drop(parsed);
    log(t0, "events deduped");

    // ---- resolve persons shard by shard (each shard's table stays in cache)
    let events: Vec<Vec<Vec<Ev>>> = (0..SHARDS)
        .into_par_iter()
        .map(|sh| {
            let mut out: Vec<Vec<Ev>> = (0..PERSON_BUCKETS).map(|_| Vec::new()).collect();
            for part in &survivors {
                for e in &part[sh] {
                    let key = people.key_packed(e.up, phash(e.up));
                    out[pbucket(key)].push(Ev { key, up: e.up, ts: e.ts, price: e.price, cat: e.cat, typ: e.typ });
                }
            }
            out
        })
        .collect();
    drop(survivors);
    log(t0, "event persons resolved");

    // ---- clicks (copies are identical, so duplicates never change credit or assists)
    let cch = chunks(&data_dir.join("clicks"));
    let cparsed: Vec<Vec<Vec<ClkU>>> = cch
        .par_iter()
        .map(|&c| {
            let mut out: Vec<Vec<ClkU>> = (0..SHARDS).map(|_| Vec::new()).collect();
            let mut f: [S; 4] = [&[]; 4];
            for_each_line(c, |line| {
                if !fields(line, &mut f) || f[0].is_empty() || f[1].is_empty() {
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
            out
        })
        .collect();
    let clicks: Vec<Vec<Vec<Clk>>> = (0..SHARDS)
        .into_par_iter()
        .map(|sh| {
            let mut out: Vec<Vec<Clk>> = (0..PERSON_BUCKETS).map(|_| Vec::new()).collect();
            for part in &cparsed {
                for c in &part[sh] {
                    let key = people.key_packed(c.up, phash(c.up));
                    out[pbucket(key)].push(Clk { key, ts: c.ts, cid: c.cid, camp: c.camp });
                }
            }
            out
        })
        .collect();
    drop(cparsed);
    log(t0, "clicks parsed");

    // ---- per person bucket: sessions, user rows, credit and assists
    let res: Vec<(Vec<Vec<UserRow>>, Acc)> = (0..PERSON_BUCKETS)
        .into_par_iter()
        .fold(
            || (Vec::new(), vec![[0i64; 4]; (ncamp + 1) * ndays]),
            |(mut rv, mut acc), b| {
                let mut evs: Vec<Ev> = events.iter().flat_map(|d| d[b].iter().copied()).collect();
                let mut cl: Vec<Clk> = clicks.iter().flat_map(|d| d[b].iter().copied()).collect();
                let mut rows = Vec::new();
                process_bucket(&ctx, &people, &long_names, &mut evs, &mut cl, &mut rows, &mut acc);
                rv.push(rows);
                (rv, acc)
            },
        )
        .collect();
    drop(events);
    drop(clicks);
    let mut rows: Vec<Vec<UserRow>> = Vec::new();
    let mut acc = vec![[0i64; 4]; (ncamp + 1) * ndays];
    for (r, a) in res {
        rows.extend(r);
        for (x, y) in acc.iter_mut().zip(a.iter()) {
            for k in 0..4 {
                x[k] += y[k];
            }
        }
    }
    log(t0, &format!("sessions done: {} user rows", rows.iter().map(|r| r.len()).sum::<usize>()));

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

    let nfiles = rayon::current_num_threads().max(1);
    let mut groups: Vec<Vec<Vec<UserRow>>> = (0..nfiles).map(|_| Vec::new()).collect();
    for (i, r) in rows.drain(..).enumerate() {
        groups[i % nfiles].push(r);
    }
    let catnames = &catnames;
    groups.into_par_iter().enumerate().for_each(|(fi, g)| {
        let rs: Vec<&UserRow> = g.iter().flatten().collect();
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

    let mut cr: Vec<(usize, usize, [i64; 4])> = Vec::new();
    for c in 0..=ncamp {
        for d in 0..ndays {
            let a = acc[c * ndays + d];
            if a[0] > 0 || a[3] > 0 {
                cr.push((c, d, a));
            }
        }
    }
    {
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
    }
    log(t0, "done");
    // skip destructors of large structures
    std::process::exit(0);
}
