#!/usr/bin/env python3
"""
NSE Backend Monitor
==================

Monitor and analyze the NSE backend performance and logs.
"""

import re
import time
import requests
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import json


class BackendMonitor:
    """Monitor NSE backend performance"""
    
    def __init__(self, api_base_url="http://localhost:8000", log_file="nse_backend.log"):
        self.api_base_url = api_base_url
        self.log_file = log_file
        self.stats = {
            "total_cycles": 0,
            "successful_cycles": 0,
            "failed_cycles": 0,
            "api_errors": 0,
            "json_errors": 0,
            "unicode_errors": 0,
            "avg_cycle_time": 0,
            "last_success": None,
            "error_patterns": Counter()
        }
    
    def check_api_health(self):
        """Check if the API is running and healthy"""
        
        try:
            response = requests.get(f"{self.api_base_url}/health", timeout=5)
            if response.status_code == 200:
                data = response.json()
                return {
                    "status": "healthy",
                    "api_running": True,
                    "database_connected": data.get("database_connected", False),
                    "total_records": data.get("total_records", 0),
                    "last_data_fetch": data.get("last_data_fetch")
                }
            else:
                return {"status": "unhealthy", "api_running": True, "status_code": response.status_code}
        except requests.exceptions.ConnectionError:
            return {"status": "down", "api_running": False}
        except Exception as e:
            return {"status": "error", "api_running": False, "error": str(e)}
    
    def get_pipeline_status(self):
        """Get pipeline status from API"""
        
        try:
            response = requests.get(f"{self.api_base_url}/pipeline/status", timeout=5)
            if response.status_code == 200:
                return response.json().get("data", {})
            return None
        except:
            return None
    
    def analyze_logs(self, lines_to_read=1000):
        """Analyze recent log entries"""
        
        try:
            with open(self.log_file, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
                recent_lines = lines[-lines_to_read:] if len(lines) > lines_to_read else lines
            
            # Reset stats
            self.stats = {
                "total_cycles": 0,
                "successful_cycles": 0,
                "failed_cycles": 0,
                "api_errors": 0,
                "json_errors": 0,
                "unicode_errors": 0,
                "cycle_times": [],
                "last_success": None,
                "error_patterns": Counter(),
                "recent_errors": []
            }
            
            for line in recent_lines:
                self._analyze_log_line(line)
            
            # Calculate averages
            if self.stats["cycle_times"]:
                self.stats["avg_cycle_time"] = sum(self.stats["cycle_times"]) / len(self.stats["cycle_times"])
            
            return self.stats
            
        except Exception as e:
            return {"error": f"Failed to analyze logs: {e}"}
    
    def _analyze_log_line(self, line):
        """Analyze a single log line"""
        
        # Success patterns
        if "[SUCCESS] Cycle completed" in line:
            self.stats["successful_cycles"] += 1
            self.stats["total_cycles"] += 1
            
            # Extract cycle time
            time_match = re.search(r'in ([\d.]+)s', line)
            if time_match:
                self.stats["cycle_times"].append(float(time_match.group(1)))
            
            # Extract timestamp
            timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
            if timestamp_match:
                self.stats["last_success"] = timestamp_match.group(1)
        
        # Failure patterns
        elif "[FAILED] Cycle failed" in line:
            self.stats["failed_cycles"] += 1
            self.stats["total_cycles"] += 1
            
            # Extract error details
            if "after" in line and ":" in line:
                error_part = line.split(":", 2)[-1].strip()
                self.stats["recent_errors"].append(error_part[:100])  # Truncate long errors
        
        # Specific error patterns
        if "Expecting value: line 1 column 1" in line:
            self.stats["json_errors"] += 1
            self.stats["error_patterns"]["JSON Parse Error"] += 1
        
        if "Failed to fetch option chain" in line:
            self.stats["api_errors"] += 1
            self.stats["error_patterns"]["API Fetch Error"] += 1
        
        if "UnicodeEncodeError" in line:
            self.stats["unicode_errors"] += 1
            self.stats["error_patterns"]["Unicode Error"] += 1
        
        if "Attempt" in line and "failed" in line:
            self.stats["error_patterns"]["Retry Attempt"] += 1
    
    def get_recommendations(self):
        """Get recommendations based on current status"""
        
        recommendations = []
        
        # Analyze API health
        health = self.check_api_health()
        if not health.get("api_running"):
            recommendations.append("🔴 API is not running. Start with: python nse_backend_app.py")
        elif health.get("status") != "healthy":
            recommendations.append("🟡 API is running but unhealthy. Check logs for errors.")
        
        # Analyze log stats
        stats = self.analyze_logs()
        
        if stats.get("total_cycles", 0) > 0:
            success_rate = (stats["successful_cycles"] / stats["total_cycles"]) * 100
            
            if success_rate < 50:
                recommendations.append("🔴 Low success rate (<50%). Consider increasing fetch interval.")
            elif success_rate < 80:
                recommendations.append("🟡 Moderate success rate. NSE API may be having issues.")
        
        if stats.get("json_errors", 0) > 10:
            recommendations.append("🟡 Many JSON parse errors. NSE API is returning invalid responses.")
        
        if stats.get("unicode_errors", 0) > 0:
            recommendations.append("🟡 Unicode encoding issues detected. Update logging configuration.")
        
        if stats.get("avg_cycle_time", 0) > 10:
            recommendations.append("🟡 Slow cycle times. Consider optimizing or increasing timeout.")
        
        if not recommendations:
            recommendations.append("✅ System appears to be running well!")
        
        return recommendations
    
    def print_status_report(self):
        """Print comprehensive status report"""
        
        print("=" * 80)
        print("NSE BACKEND MONITORING REPORT")
        print("=" * 80)
        print(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # API Health
        print("\n🌐 API HEALTH:")
        health = self.check_api_health()
        for key, value in health.items():
            print(f"   {key}: {value}")
        
        # Pipeline Status
        print("\n⚙️ PIPELINE STATUS:")
        pipeline_status = self.get_pipeline_status()
        if pipeline_status:
            print(f"   Running: {pipeline_status.get('running', 'Unknown')}")
            print(f"   Total Cycles: {pipeline_status.get('total_cycles', 0)}")
            print(f"   Success Rate: {pipeline_status.get('success_rate', 0):.1f}%")
            print(f"   Symbols: {pipeline_status.get('symbols', [])}")
        else:
            print("   Unable to get pipeline status")
        
        # Log Analysis
        print("\n📊 LOG ANALYSIS:")
        stats = self.analyze_logs()
        if "error" not in stats:
            print(f"   Total Cycles: {stats['total_cycles']}")
            print(f"   Successful: {stats['successful_cycles']}")
            print(f"   Failed: {stats['failed_cycles']}")
            if stats['total_cycles'] > 0:
                success_rate = (stats['successful_cycles'] / stats['total_cycles']) * 100
                print(f"   Success Rate: {success_rate:.1f}%")
            print(f"   Average Cycle Time: {stats.get('avg_cycle_time', 0):.2f}s")
            print(f"   Last Success: {stats.get('last_success', 'Never')}")
            
            if stats['error_patterns']:
                print("\n   Error Patterns:")
                for error, count in stats['error_patterns'].most_common(5):
                    print(f"     {error}: {count}")
        else:
            print(f"   {stats['error']}")
        
        # Recommendations
        print("\n💡 RECOMMENDATIONS:")
        recommendations = self.get_recommendations()
        for rec in recommendations:
            print(f"   {rec}")
        
        print("\n" + "=" * 80)
    
    def monitor_continuously(self, interval=30):
        """Monitor continuously with periodic reports"""
        
        print(f"Starting continuous monitoring (reporting every {interval}s)")
        print("Press Ctrl+C to stop")
        
        try:
            while True:
                self.print_status_report()
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\nMonitoring stopped by user")


def main():
    """Main monitoring function"""
    
    import argparse
    
    parser = argparse.ArgumentParser(description="Monitor NSE Backend")
    parser.add_argument("--api-url", default="http://localhost:8000", help="API base URL")
    parser.add_argument("--log-file", default="nse_backend.log", help="Log file path")
    parser.add_argument("--continuous", action="store_true", help="Run continuous monitoring")
    parser.add_argument("--interval", type=int, default=30, help="Monitoring interval in seconds")
    
    args = parser.parse_args()
    
    monitor = BackendMonitor(args.api_url, args.log_file)
    
    if args.continuous:
        monitor.monitor_continuously(args.interval)
    else:
        monitor.print_status_report()


if __name__ == "__main__":
    main()
