use clap::Parser;
use nix::sys::ptrace;
use nix::sys::wait::wait;
use nix::unistd::Pid;
use reqwest::Client;
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::fs::File;
use std::io;
use std::path::{PathBuf};
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use tokio::time::sleep;
use tracing::{error, info, warn};
use nix::sys::ptrace::Options;
use std::os::raw::c_long;
use serde::Serialize;
use std::io::{BufReader, SeekFrom, Read, Seek, Write};
use std::fs::OpenOptions;
use tempfile::NamedTempFile; 

#[derive(Parser, Debug, Serialize)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// PID of the target process
    #[arg(short, long)]
    pid: i32,

    /// Loki server address
    #[arg(short, long)]
    loki_url: String,

    /// Cache file path for storing failed pushes
    #[arg(short, long, default_value = "/tmp")]
    cache_file_path: String,

    /// Maximum cache size in memory (in bytes)
    #[arg(short, long, default_value_t = 1024 * 1024)] // 1MB default
    memory_cache_size_max: usize,

    /// Maximum cache size on disk (in bytes)
    #[arg(short, long, default_value_t = 1024 * 1024 * 10)] // 10MB default
    disk_cache_size_max: usize,
}

#[derive(Debug)]
struct BackoffStrategy {
    attempt: u32,
    base_delay: Duration,
    max_delay: Duration,
}

impl BackoffStrategy {
    fn new() -> Self {
        Self {
            attempt: 0,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(300), // 5 minutes max
        }
    }

    fn next_delay(&mut self) -> Duration {
        let delay = self.base_delay * 2u32.pow(self.attempt);
        self.attempt += 1;
        std::cmp::min(delay, self.max_delay)
    }

    fn reset(&mut self) {
        self.attempt = 0;
    }
}

struct LokiCache {
    memory_cache: VecDeque<Value>,
    memory_cache_size: usize,
    disk_cache_file: PathBuf,
    disk_cache_size_max: usize,
}

impl LokiCache {
    fn new(memory_cache_size: usize, cache_file_path: String, disk_cache_size_max: usize) -> io::Result<Self> {
        let mut memory_cache = VecDeque::new();
        let mut disk_cache_size: usize = 0; 
        

        // Create a temporary file
        let temp_file = NamedTempFile::with_prefix_in("loki_cache", cache_file_path)?;
        
        let disk_cache_file = temp_file.path().to_path_buf(); 

        info!("Cache file path {}",temp_file.path().display());

        if disk_cache_file.exists() {
            let file = File::open(&disk_cache_file)?;
            let mut reader = BufReader::new(file);
            loop {
                let mut buf = [0u8; 1024];
                let bytes_read = reader.read(&mut buf)?;

                if bytes_read == 0 {
                    break;
                }

                disk_cache_size += bytes_read;
                if disk_cache_size > disk_cache_size_max {
                    error!("Disk cache size exceeded limit. Truncating.");
                    let mut file = OpenOptions::new()
                        .write(true)
                        .truncate(true)
                        .open(&disk_cache_file)?;
                    file.seek(SeekFrom::Start(0))?;
                    break;
                }

                let entry_str = String::from_utf8_lossy(&buf[..bytes_read]);
                if let Ok(entry) = serde_json::from_str(&entry_str) {
                    memory_cache.push_back(entry);
                } else {
                    warn!("Failed to deserialize entry from disk cache.");
                }
            }
        }

        Ok(Self {
            memory_cache,
            memory_cache_size,
            disk_cache_file,
            disk_cache_size_max,
        })
    }

    fn push(&mut self, entry: Value) {
        let entry_bytes = serde_json::to_vec(&entry).unwrap(); 

        // Check memory cache size
        let mut current_memory_size: usize = 0;
        for entry in &self.memory_cache {
            current_memory_size += serde_json::to_vec(entry).unwrap().len();
        }

        if current_memory_size + entry_bytes.len() <= self.memory_cache_size {
            self.memory_cache.push_back(entry);
        } else {
            // Flush to disk
            self.flush_to_disk(); 

            // Try to add to memory cache again
            if current_memory_size + entry_bytes.len() <= self.memory_cache_size {
                self.memory_cache.push_back(entry);
            } else {
                // If still exceeds memory limit, directly write to disk
                self.append_to_disk(&entry_bytes);
            }
        }
    }

    fn flush_to_disk(&mut self) {
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(&self.disk_cache_file)
            .unwrap();

        let mut current_disk_size: usize = 0;
        while let Some(entry) = self.memory_cache.pop_front() {
            let entry_bytes = serde_json::to_vec(&entry).unwrap();
            current_disk_size += entry_bytes.len();

            if current_disk_size > self.disk_cache_size_max {
                // Truncate disk cache if size exceeds limit
                file.set_len(0).unwrap();
                current_disk_size = 0;
            }

            file.write_all(&entry_bytes).unwrap();
        }
    }

    fn append_to_disk(&self, entry_bytes: &[u8]) {
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(&self.disk_cache_file)
            .unwrap();

        // Check disk cache size
        let mut current_disk_size: usize = 0;
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut reader = BufReader::new(file.try_clone().unwrap());
        loop {
            let mut buf = [0u8; 1024];
            let bytes_read = reader.read(&mut buf).unwrap();

            if bytes_read == 0 {
                break;
            }

            current_disk_size += bytes_read;
        }

        if current_disk_size + entry_bytes.len() > self.disk_cache_size_max {
            // Truncate disk cache if size exceeds limit
            file.set_len(0).unwrap();
        }

        file.write_all(entry_bytes).unwrap();
    }

    fn pop(&mut self) -> Option<Value> {
        if let Some(entry) = self.memory_cache.pop_front() {
            Some(entry)
        } else {
            // Read from disk
            let file = match File::open(&self.disk_cache_file) {
                Ok(file) => file,
                Err(e) => {
                    if e.kind() == io::ErrorKind::NotFound {
                        warn!("Disk cache file not found: {}", e);
                        return None; 
                    } else {
                        error!("Failed to open disk cache file: {}", e);
                        return None;
                    }
                },
            };
            let mut reader = BufReader::new(file.try_clone().unwrap());
            let mut buf = String::new();
            if reader.read_to_string(&mut buf).is_ok() {
                if let Ok(entry) = serde_json::from_str(&buf) {
                    // Remove the entry from disk
                    file.set_len(0).unwrap(); 
                    Some(entry)
                } else {
                    warn!("Failed to deserialize entry from disk cache.");
                    None
                }
            } else {
                None
            }
        }
    }
}

struct LokiClient {
    client: Client,
    url: String,
    cache: LokiCache,
    backoff: BackoffStrategy,
}

impl LokiClient {
    fn new(url: String, cache: LokiCache) -> Self {
        Self {
            client: Client::new(),
            url,
            cache,
            backoff: BackoffStrategy::new(),
        }
    }

    async fn push_entry(&mut self, line: String, timestamp: i64) -> Result<(), Box<dyn std::error::Error>> {
        let entry = json!({
            "streams": [{
                "stream": {
                    "source": "stdout_capture"
                },
                "values": [[
                    timestamp.to_string(),
                    line
                ]]
            }]
        });

        println!("pushed to loki {} {}", entry, timestamp.to_string());

        match self.push_to_loki(&entry).await {
            Ok(_) => {
                self.backoff.reset();
                self.try_push_cached_entries().await;
                println!("pushed to loki");
                Ok(())
            }
            Err(e) => {
                warn!("Failed to push to Loki: {}", e);
                self.cache.push(entry);
                let delay = self.backoff.next_delay();
                sleep(delay).await;
                Err(e)
            }
        }
    }

    async fn push_to_loki(&self, entry: &Value) -> Result<(), Box<dyn std::error::Error>> {
        println!("{}",entry);
        let response = self.client
            .post(&self.url)
            .json(entry)
            .send()
            .await?;

        println!("Loki responded with status: {}", response.status());  
        if !response.status().is_success() {
            return Err(format!("Loki responded with status: {}", response.status()).into());
        }

        Ok(())
    }

    async fn try_push_cached_entries(&mut self) {
        while let Some(entry) = self.cache.pop() {
            if let Err(e) = self.push_to_loki(&entry).await {
                warn!("Failed to push cached entry: {}", e);
                self.cache.push(entry);
                break;
            }
        }
    }
}

struct SyscallTracer {
    pid: Pid,
}

impl SyscallTracer {
    fn new(pid: Pid) -> Result<Self, Box<dyn std::error::Error>> {
        // Attach to the process
        ptrace::attach(pid)?;
        wait()?;

        // Set ptrace options to trace syscalls
        ptrace::setoptions(pid, Options::PTRACE_O_TRACESYSGOOD)?;

        Ok(Self { pid })
    }

    fn get_syscall_number(&self) -> Result<c_long, Box<dyn std::error::Error>> {
        #[cfg(target_arch = "x86_64")]
        let _syscall_reg = nix::libc::ORIG_RAX;
        
        let regs = ptrace::getregs(self.pid)?;
        Ok(regs.orig_rax as c_long)
    }

    fn get_write_contents(&self) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let regs = ptrace::getregs(self.pid)?;
        
        // On x86_64, write syscall arguments are:
        // rdi: fd
        // rsi: buffer pointer
        // rdx: count
        let fd = regs.rdi as i32;
        let buf_ptr = regs.rsi as usize;
        let count = regs.rdx as usize;

        // Only capture stdout (fd 1)
        if fd != 1 {
            return Ok(Vec::new());
        }

        let mut contents = Vec::with_capacity(count);
        let mut offset = 0;
        while offset < count {
            let word = ptrace::read(self.pid, (buf_ptr + offset) as ptrace::AddressType)? as u64;
            let bytes: [u8; 8] = word.to_ne_bytes();
            let remaining = count - offset;
            let to_take = std::cmp::min(remaining, 8);
            contents.extend_from_slice(&bytes[..to_take]);
            offset += 8;
        }

        Ok(contents)
    }

    async fn trace_writes(&self, loki_client: &mut LokiClient) -> Result<(), Box<dyn std::error::Error>> {
        loop {
            // Wait for next syscall entry
            ptrace::syscall(self.pid, None)?;
            wait()?;

            // Check if it's a write syscall
            if self.get_syscall_number()? == libc::SYS_write as c_long {
                // Get the write contents before the syscall completes
                let contents = self.get_write_contents()?;

                // Let the syscall complete
                ptrace::syscall(self.pid, None)?;
                wait()?;

                if !contents.is_empty() {
                    // Convert contents to string and send to Loki
                    if let Ok(line) = String::from_utf8(contents) {
                        // Get the current instant in time
                        let unix_timestamp = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Time went backwards");

                        println!("timetamp {}", unix_timestamp.as_secs());
                        loki_client.push_entry(line, unix_timestamp.as_nanos() as i64).await?;
                        println!("timetamp {}", SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Time went backwards").as_secs());
                    }
                }
            } else {
                // Let non-write syscalls complete
                ptrace::syscall(self.pid, None)?;
                wait()?;
            }
        }
    }
}

impl Drop for SyscallTracer {
    fn drop(&mut self) {
        // Detach from the process when the tracer is dropped
        let _ = ptrace::detach(self.pid, None);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Parse command line arguments
    let args = Args::parse();
    println!("{}", serde_json::to_string_pretty(&args).unwrap());
    // Initialize Loki client with cache
    let cache = LokiCache::new(args.memory_cache_size_max, args.cache_file_path, args.disk_cache_size_max)?;
    let mut loki_client = LokiClient::new(args.loki_url, cache);

    // Create syscall tracer
    let pid = Pid::from_raw(args.pid);
    let tracer = SyscallTracer::new(pid)?;

    // Start tracing write syscalls
    info!("Started tracing process {}", args.pid);
    tracer.trace_writes(&mut loki_client).await?;

    Ok(())
}
