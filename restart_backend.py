#!/usr/bin/env python3
"""
Backend Restart Script
======================

Script to restart the NSE backend with improved settings and monitoring.
"""

import os
import sys
import time
import subprocess
import signal
import psutil
import requests
from datetime import datetime


def find_backend_processes():
    """Find running backend processes"""
    
    processes = []
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmdline = ' '.join(proc.info['cmdline']) if proc.info['cmdline'] else ''
            if 'nse_backend_app.py' in cmdline or 'python' in proc.info['name'] and 'nse_backend' in cmdline:
                processes.append(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    
    return processes


def stop_backend():
    """Stop running backend processes"""
    
    print("🔍 Looking for running backend processes...")
    processes = find_backend_processes()
    
    if not processes:
        print("✅ No backend processes found running")
        return True
    
    print(f"🛑 Found {len(processes)} backend process(es). Stopping...")
    
    for proc in processes:
        try:
            print(f"   Stopping PID {proc.pid}: {' '.join(proc.cmdline())}")
            proc.terminate()
            
            # Wait for graceful shutdown
            try:
                proc.wait(timeout=10)
                print(f"   ✅ Process {proc.pid} stopped gracefully")
            except psutil.TimeoutExpired:
                print(f"   🔨 Force killing process {proc.pid}")
                proc.kill()
                proc.wait()
                
        except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
            print(f"   ⚠️  Could not stop process {proc.pid}: {e}")
    
    # Verify all processes are stopped
    time.sleep(2)
    remaining = find_backend_processes()
    if remaining:
        print(f"⚠️  {len(remaining)} processes still running")
        return False
    else:
        print("✅ All backend processes stopped")
        return True


def check_dependencies():
    """Check if required dependencies are available"""
    
    print("🔍 Checking dependencies...")
    
    required_modules = ['fastapi', 'uvicorn', 'pymongo', 'requests', 'pydantic']
    missing = []
    
    for module in required_modules:
        try:
            __import__(module)
            print(f"   ✅ {module}")
        except ImportError:
            print(f"   ❌ {module}")
            missing.append(module)
    
    if missing:
        print(f"\n❌ Missing dependencies: {', '.join(missing)}")
        print("Install with: pip install " + " ".join(missing))
        return False
    
    print("✅ All dependencies available")
    return True


def start_backend():
    """Start the backend with improved settings"""
    
    print("🚀 Starting NSE backend...")
    
    # Set environment variables for better performance
    env = os.environ.copy()
    env['PYTHONUNBUFFERED'] = '1'  # Unbuffered output
    env['PYTHONIOENCODING'] = 'utf-8'  # UTF-8 encoding
    
    try:
        # Start the backend process
        process = subprocess.Popen(
            [sys.executable, 'nse_backend_app.py'],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            universal_newlines=True,
            bufsize=1
        )
        
        print(f"✅ Backend started with PID {process.pid}")
        
        # Wait a moment for startup
        time.sleep(3)
        
        # Check if process is still running
        if process.poll() is None:
            print("✅ Backend process is running")
            
            # Test API connectivity
            print("🔍 Testing API connectivity...")
            for attempt in range(5):
                try:
                    response = requests.get("http://localhost:8000/health", timeout=5)
                    if response.status_code == 200:
                        print("✅ API is responding")
                        return True
                    else:
                        print(f"⚠️  API returned status {response.status_code}")
                except requests.exceptions.ConnectionError:
                    print(f"   Attempt {attempt + 1}/5: API not ready yet...")
                    time.sleep(2)
                except Exception as e:
                    print(f"   API test error: {e}")
                    time.sleep(2)
            
            print("⚠️  API is not responding, but process is running")
            return True
        else:
            print("❌ Backend process failed to start")
            return False
            
    except Exception as e:
        print(f"❌ Failed to start backend: {e}")
        return False


def show_status():
    """Show current backend status"""
    
    print("\n📊 CURRENT STATUS:")
    
    # Check processes
    processes = find_backend_processes()
    print(f"   Running processes: {len(processes)}")
    
    # Check API
    try:
        response = requests.get("http://localhost:8000/health", timeout=5)
        if response.status_code == 200:
            data = response.json()
            print("   API Status: ✅ Healthy")
            print(f"   Database: {'✅ Connected' if data.get('database_connected') else '❌ Disconnected'}")
            print(f"   Total Records: {data.get('total_records', 0)}")
        else:
            print(f"   API Status: ⚠️  Unhealthy (Status {response.status_code})")
    except requests.exceptions.ConnectionError:
        print("   API Status: ❌ Not responding")
    except Exception as e:
        print(f"   API Status: ❌ Error ({e})")
    
    # Check log file
    if os.path.exists('nse_backend.log'):
        stat = os.stat('nse_backend.log')
        size_mb = stat.st_size / (1024 * 1024)
        mod_time = datetime.fromtimestamp(stat.st_mtime)
        print(f"   Log File: {size_mb:.1f} MB, last modified {mod_time.strftime('%H:%M:%S')}")
    else:
        print("   Log File: ❌ Not found")


def main():
    """Main restart function"""
    
    print("🔄 NSE Backend Restart Script")
    print("=" * 50)
    
    # Check dependencies first
    if not check_dependencies():
        sys.exit(1)
    
    # Stop existing processes
    if not stop_backend():
        print("❌ Failed to stop existing processes")
        sys.exit(1)
    
    # Start backend
    if not start_backend():
        print("❌ Failed to start backend")
        sys.exit(1)
    
    # Show final status
    show_status()
    
    print("\n✅ Backend restart completed!")
    print("\n📖 Next steps:")
    print("   • Monitor logs: tail -f nse_backend.log")
    print("   • Check API docs: http://localhost:8000/docs")
    print("   • Monitor status: python monitor_backend.py")
    print("   • View data: http://localhost:8000/option-chain/NIFTY")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⏹️  Restart cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Restart failed: {e}")
        sys.exit(1)
