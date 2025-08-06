#!/usr/bin/env python3
"""
CocoPan Monitor - System Health Check
Comprehensive health monitoring for all system components
"""
import os
import sys
import json
import time
import requests
import subprocess
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any
import pytz

from config import config
from database import db

class HealthChecker:
    """Comprehensive system health checker"""
    
    def __init__(self):
        self.results = {}
        self.manila_tz = pytz.timezone(config.TIMEZONE)
        
    def check_database_health(self) -> Dict[str, Any]:
        """Check database connectivity and performance"""
        try:
            start_time = time.time()
            
            # Test connection
            stats = db.get_database_stats()
            connection_time = time.time() - start_time
            
            if not stats:
                return {
                    'status': 'ERROR',
                    'message': 'Could not retrieve database stats',
                    'response_time': connection_time
                }
            
            # Check recent data
            latest_status = db.get_latest_status()
            recent_checks = len(latest_status) if latest_status is not None else 0
            
            # Check if data is recent (within last 2 hours)
            if recent_checks > 0:
                try:
                    latest_check_time = latest_status['checked_at'].max()
                    if isinstance(latest_check_time, str):
                        latest_check_time = datetime.fromisoformat(latest_check_time.replace('Z', '+00:00'))
                    
                    time_since_last = datetime.now(pytz.UTC) - latest_check_time.replace(tzinfo=pytz.UTC)
                    data_freshness = time_since_last.total_seconds() / 3600  # hours
                except:
                    data_freshness = 999  # Unknown
            else:
                data_freshness = 999
            
            # Determine status
            if connection_time > 5:
                status = 'SLOW'
                message = f'Database responding slowly ({connection_time:.1f}s)'
            elif data_freshness > 2:
                status = 'STALE'
                message = f'Data is {data_freshness:.1f} hours old'
            elif recent_checks == 0:
                status = 'NO_DATA'
                message = 'No store data found'
            else:
                status = 'HEALTHY'
                message = f'{stats["store_count"]} stores, {recent_checks} with recent status'
            
            return {
                'status': status,
                'message': message,
                'response_time': connection_time,
                'store_count': stats.get('store_count', 0),
                'total_checks': stats.get('total_checks', 0),
                'recent_checks': recent_checks,
                'data_freshness_hours': data_freshness,
                'db_type': stats.get('db_type', 'unknown')
            }
            
        except Exception as e:
            return {
                'status': 'ERROR',
                'message': f'Database health check failed: {str(e)}',
                'response_time': None
            }
    
    def check_monitor_service(self) -> Dict[str, Any]:
        """Check if monitor service is running and functioning"""
        try:
            # Check if it's monitoring time
            current_time = datetime.now(self.manila_tz)
            current_hour = current_time.hour
            is_monitoring_time = config.is_monitor_time(current_hour)
            
            # Check for recent monitoring activity
            try:
                store_logs = db.get_store_logs(limit=10)
                if len(store_logs) > 0:
                    latest_log_time = store_logs['checked_at'].max()
                    if isinstance(latest_log_time, str):
                        latest_log_time = datetime.fromisoformat(latest_log_time.replace('Z', '+00:00'))
                    
                    time_since_last = datetime.now(pytz.UTC) - latest_log_time.replace(tzinfo=pytz.UTC)
                    minutes_since_last = time_since_last.total_seconds() / 60
                else:
                    minutes_since_last = 999
            except:
                minutes_since_last = 999
            
            # Check if process is running (basic check)
            monitor_running = self._is_monitor_process_running()
            
            # Determine status
            if not is_monitoring_time:
                status = 'IDLE'
                message = f'Outside monitoring hours ({config.MONITOR_START_HOUR}:00-{config.MONITOR_END_HOUR}:00)'
            elif not monitor_running:
                status = 'STOPPED'
                message = 'Monitor service process not detected'
            elif minutes_since_last > 90:  # More than 1.5 hours
                status = 'STALE'
                message = f'No monitoring activity for {minutes_since_last:.0f} minutes'
            elif minutes_since_last > 70:  # More than expected interval
                status = 'DELAYED'
                message = f'Last check was {minutes_since_last:.0f} minutes ago'
            else:
                status = 'ACTIVE'
                message = f'Last check {minutes_since_last:.0f} minutes ago'
            
            return {
                'status': status,
                'message': message,
                'is_monitoring_time': is_monitoring_time,
                'minutes_since_last_check': minutes_since_last,
                'process_running': monitor_running,
                'monitoring_hours': f'{config.MONITOR_START_HOUR}:00-{config.MONITOR_END_HOUR}:00 {config.TIMEZONE}'
            }
            
        except Exception as e:
            return {
                'status': 'ERROR',
                'message': f'Monitor service check failed: {str(e)}'
            }
    
    def check_dashboard_health(self) -> Dict[str, Any]:
        """Check dashboard accessibility and responsiveness"""
        try:
            start_time = time.time()
            
            # Test dashboard health endpoint
            response = requests.get(
                f'http://localhost:{config.DASHBOARD_PORT}/_stcore/health',
                timeout=10
            )
            
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                status = 'HEALTHY'
                message = f'Dashboard responding in {response_time:.1f}s'
            else:
                status = 'ERROR'
                message = f'Dashboard returned status {response.status_code}'
            
            return {
                'status': status,
                'message': message,
                'response_time': response_time,
                'url': f'http://localhost:{config.DASHBOARD_PORT}'
            }
            
        except requests.exceptions.ConnectionError:
            return {
                'status': 'OFFLINE',
                'message': 'Dashboard not accessible',
                'url': f'http://localhost:{config.DASHBOARD_PORT}'
            }
        except Exception as e:
            return {
                'status': 'ERROR',
                'message': f'Dashboard health check failed: {str(e)}'
            }
    
    def check_docker_services(self) -> Dict[str, Any]:
        """Check Docker services status"""
        try:
            # Check if Docker is available
            result = subprocess.run(
                ['docker', 'ps', '--filter', 'name=cocopan', '--format', 'json'],
                capture_output=True, text=True, timeout=10
            )
            
            if result.returncode != 0:
                return {
                    'status': 'ERROR',
                    'message': 'Docker not available or permission denied'
                }
            
            # Parse Docker output
            containers = []
            if result.stdout.strip():
                for line in result.stdout.strip().split('\n'):
                    try:
                        container = json.loads(line)
                        containers.append({
                            'name': container.get('Names', ''),
                            'status': container.get('Status', ''),
                            'state': container.get('State', '')
                        })
                    except:
                        continue
            
            # Check expected containers
            expected_containers = ['cocopan_postgres', 'cocopan_monitor', 'cocopan_dashboard']
            running_containers = [c['name'] for c in containers if 'running' in c['status'].lower()]
            
            if len(containers) == 0:
                status = 'NO_CONTAINERS'
                message = 'No CocoPan containers found'
            elif len(running_containers) == len(expected_containers):
                status = 'HEALTHY'
                message = f'All {len(running_containers)} services running'
            else:
                status = 'PARTIAL'
                message = f'{len(running_containers)}/{len(expected_containers)} services running'
            
            return {
                'status': status,
                'message': message,
                'containers': containers,
                'running_count': len(running_containers),
                'expected_count': len(expected_containers)
            }
            
        except subprocess.TimeoutExpired:
            return {
                'status': 'TIMEOUT',
                'message': 'Docker command timed out'
            }
        except FileNotFoundError:
            return {
                'status': 'NOT_AVAILABLE',
                'message': 'Docker not installed or not in PATH'
            }
        except Exception as e:
            return {
                'status': 'ERROR',
                'message': f'Docker check failed: {str(e)}'
            }
    
    def check_system_resources(self) -> Dict[str, Any]:
        """Check system resource usage"""
        try:
            import psutil
            
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # Memory usage
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            
            # Disk usage
            disk = psutil.disk_usage('/')
            disk_percent = disk.percent
            
            # Load average (Unix-like systems)
            try:
                load_avg = os.getloadavg()[0]  # 1-minute load average
            except (OSError, AttributeError):
                load_avg = None
            
            # Determine status
            if cpu_percent > 90 or memory_percent > 90 or disk_percent > 95:
                status = 'CRITICAL'
                message = 'System resources critically high'
            elif cpu_percent > 70 or memory_percent > 80 or disk_percent > 85:
                status = 'HIGH'
                message = 'System resources elevated'
            else:
                status = 'NORMAL'
                message = 'System resources normal'
            
            return {
                'status': status,
                'message': message,
                'cpu_percent': cpu_percent,
                'memory_percent': memory_percent,
                'disk_percent': disk_percent,
                'load_average': load_avg
            }
            
        except ImportError:
            return {
                'status': 'NO_PSUTIL',
                'message': 'psutil not available for system monitoring'
            }
        except Exception as e:
            return {
                'status': 'ERROR',
                'message': f'System resource check failed: {str(e)}'
            }
    
    def _is_monitor_process_running(self) -> bool:
        """Check if monitor process is running (basic check)"""
        try:
            import psutil
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                cmdline = proc.info.get('cmdline', [])
                if any('monitor_service.py' in cmd for cmd in cmdline):
                    return True
            return False
        except:
            # Fallback to simpler check
            return os.path.exists('cocopan_monitor.log')
    
    def run_all_checks(self) -> Dict[str, Any]:
        """Run all health checks and return comprehensive report"""
        print("🔍 Running CocoPan system health checks...")
        
        checks = {
            'database': self.check_database_health(),
            'monitor_service': self.check_monitor_service(),
            'dashboard': self.check_dashboard_health(),
            'docker_services': self.check_docker_services(),
            'system_resources': self.check_system_resources()
        }
        
        # Overall system status
        statuses = [check['status'] for check in checks.values()]
        
        if any(status == 'ERROR' for status in statuses):
            overall_status = 'ERROR'
        elif any(status == 'CRITICAL' for status in statuses):
            overall_status = 'CRITICAL'
        elif any(status in ['STALE', 'SLOW', 'DELAYED', 'PARTIAL'] for status in statuses):
            overall_status = 'WARNING'
        elif any(status in ['IDLE', 'OFFLINE'] for status in statuses):
            overall_status = 'IDLE'
        else:
            overall_status = 'HEALTHY'
        
        return {
            'timestamp': datetime.now(self.manila_tz).isoformat(),
            'overall_status': overall_status,
            'checks': checks
        }
    
    def print_health_report(self, results: Dict[str, Any] = None):
        """Print formatted health report"""
        if results is None:
            results = self.run_all_checks()
        
        print("\n" + "="*60)
        print(f"🏥 CocoPan System Health Report")
        print(f"⏰ {results['timestamp']}")
        print("="*60)
        
        # Overall status
        status_emoji = {
            'HEALTHY': '✅',
            'WARNING': '⚠️',
            'CRITICAL': '🔴',
            'ERROR': '❌',
            'IDLE': '😴'
        }
        
        overall = results['overall_status']
        print(f"🏥 Overall Status: {status_emoji.get(overall, '❓')} {overall}")
        print()
        
        # Individual checks
        for check_name, check_result in results['checks'].items():
            status = check_result['status']
            message = check_result['message']
            
            check_emoji = {
                'HEALTHY': '✅', 'ACTIVE': '🟢', 'NORMAL': '✅',
                'WARNING': '⚠️', 'DELAYED': '🟡', 'HIGH': '🟡', 'SLOW': '🟡',
                'STALE': '🟠', 'PARTIAL': '🟠',
                'CRITICAL': '🔴', 'ERROR': '❌', 'OFFLINE': '🔴',
                'IDLE': '😴', 'STOPPED': '⏹️',
                'NO_DATA': '📭', 'NO_CONTAINERS': '📦',
                'NOT_AVAILABLE': '❓', 'TIMEOUT': '⏱️', 'NO_PSUTIL': '📊'
            }
            
            print(f"{check_emoji.get(status, '❓')} {check_name.replace('_', ' ').title()}: {message}")
            
            # Additional details for some checks
            if check_name == 'database' and 'store_count' in check_result:
                print(f"   📊 {check_result['store_count']} stores, {check_result['total_checks']:,} total checks")
                if check_result.get('response_time'):
                    print(f"   ⚡ Response time: {check_result['response_time']:.2f}s")
            
            elif check_name == 'system_resources' and status in ['NORMAL', 'HIGH', 'CRITICAL']:
                print(f"   💻 CPU: {check_result['cpu_percent']:.1f}%, RAM: {check_result['memory_percent']:.1f}%, Disk: {check_result['disk_percent']:.1f}%")
            
            elif check_name == 'docker_services' and 'containers' in check_result:
                for container in check_result['containers']:
                    print(f"   🐳 {container['name']}: {container['status']}")
        
        print("\n" + "="*60)
        
        # Recommendations
        print("💡 Recommendations:")
        if overall == 'ERROR':
            print("   • Check error messages above and resolve critical issues")
            print("   • Verify database connectivity and monitor service status")
        elif overall == 'CRITICAL':
            print("   • Address critical system resource issues immediately")
            print("   • Consider scaling system resources or optimizing services")
        elif overall == 'WARNING':
            print("   • Monitor warning conditions and investigate if they persist")
            print("   • Check logs for additional details")
        else:
            print("   • System appears healthy - continue regular monitoring")
        
        print("   • Run health checks regularly: python health_check.py")
        print("   • View detailed logs: tail -f cocopan_monitor.log")
        print()

def main():
    """Main health check entry point"""
    checker = HealthChecker()
    results = checker.run_all_checks()
    checker.print_health_report(results)
    
    # Exit with appropriate code
    overall_status = results['overall_status']
    if overall_status in ['ERROR', 'CRITICAL']:
        sys.exit(1)
    elif overall_status == 'WARNING':
        sys.exit(2)
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()