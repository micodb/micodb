use std::env;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    
    // Only apply macOS-specific flags when targeting macOS
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    
    if target_os == "macos" {
        // Apply macOS-specific flags only on macOS targets
        println!("cargo:rustc-link-arg=-arch");
        
        // Get the architecture
        let target_arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
        if target_arch == "x86_64" {
            println!("cargo:rustc-link-arg=x86_64");
        } else if target_arch == "aarch64" {
            println!("cargo:rustc-link-arg=arm64");
        }
        
        println!("cargo:rustc-link-arg=-mmacosx-version-min=10.7");
        
        // Set environment variables for dependencies that might use cc-rs
        env::set_var("CFLAGS_x86_64-apple-darwin", "-mmacosx-version-min=10.7");
        env::set_var("CXXFLAGS_x86_64-apple-darwin", "-mmacosx-version-min=10.7");
    } else {
        // Clear any macOS-specific flags for non-macOS targets
        env::remove_var("CFLAGS");
        env::remove_var("CXXFLAGS");
    }
    
    // Handle any dependencies that might have platform-specific requirements
    // For example, if you're using zstd-sys:
    if env::var("CARGO_PKG_NAME").map(|s| s == "zstd-sys").unwrap_or(false) {
        if target_os != "macos" {
            // Disable macOS-specific flags for zstd-sys on non-macOS platforms
            env::set_var("ZSTD_SYS_USE_PKG_CONFIG", "1");
        }
    }
}
