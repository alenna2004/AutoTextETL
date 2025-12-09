#!/usr/bin/env python3
"""
Resource Monitor - System resource usage monitoring
"""

import psutil
import time
from typing import Dict, Any, List
from datetime import datetime
import gc

class ResourceMonitor:
    """
    Monitors system resource usage during processing
    """
    
    def __init__(self):
        self.monitoring_history: List[Dict[str, Any]] = []
        self.is_monitoring = False
        self.monitoring_interval = 1.0  # seconds
    
    def start_monitoring(self):
        """
        Start resource monitoring
        """
        self.is_monitoring = True
        self.monitoring_start_time = time.time()
        self.monitoring_history = []
    
    def stop_monitoring(self):
        """
        Stop resource monitoring
        """
        self.is_monitoring = False
    
    def get_current_usage(self) -> Dict[str, Any]:
        """
        Get current system resource usage
        """
        cpu_percent = psutil.cpu_percent(interval=0.1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        network = psutil.net_io_counters()
        
        # Get process-specific stats
        process = psutil.Process()
        proc_memory = process.memory_info()
        proc_cpu = process.cpu_percent()
        
        usage = {
            "timestamp": datetime.now().isoformat(),
            "system": {
                "cpu_percent": cpu_percent,
                "memory_total_gb": memory.total / (1024**3),
                "memory_available_gb": memory.available / (1024**3),
                "memory_used_gb": memory.used / (1024**3),
                "memory_percent": memory.percent,
                "disk_total_gb": disk.total / (1024**3),
                "disk_used_gb": disk.used / (1024**3),
                "disk_percent": disk.percent,
                "network_bytes_sent": network.bytes_sent,
                "network_bytes_recv": network.bytes_recv
            },
            "process": {
                "cpu_percent": proc_cpu,
                "memory_rss_mb": proc_memory.rss / (1024**2),
                "memory_vms_mb": proc_memory.vms / (1024**2),
                "num_threads": process.num_threads(),
                "num_fds": process.num_fds()
            }
        }
        
        if self.is_monitoring:
            self.monitoring_history.append(usage)
        
        return usage
    
    def get_average_usage(self, samples: List[Dict[str, Any]] = None) -> Dict[str, float]:
        """
        Get average resource usage from samples
        Args:
            samples: List of usage samples (uses monitoring history if None)
        Returns:
            Dict with average usage values
        """
        if samples is None:
            samples = self.monitoring_history
        
        if not samples:
            return {}
        
        avg_cpu = sum(sample["system"]["cpu_percent"] for sample in samples) / len(samples)
        avg_mem_percent = sum(sample["system"]["memory_percent"] for sample in samples) / len(samples)
        avg_disk_percent = sum(sample["system"]["disk_percent"] for sample in samples) / len(samples)
        avg_proc_cpu = sum(sample["process"]["cpu_percent"] for sample in samples) / len(samples)
        avg_proc_memory = sum(sample["process"]["memory_rss_mb"] for sample in samples) / len(samples)
        
        return {
            "avg_cpu_percent": avg_cpu,
            "avg_memory_percent": avg_mem_percent,
            "avg_disk_percent": avg_disk_percent,
            "avg_process_cpu": avg_proc_cpu,
            "avg_process_memory_mb": avg_proc_memory
        }
    
    def get_peak_usage(self, samples: List[Dict[str, Any]] = None) -> Dict[str, float]:
        """
        Get peak resource usage from samples
        """
        if samples is None:
            samples = self.monitoring_history
        
        if not samples:
            return {}
        
        peak_cpu = max(sample["system"]["cpu_percent"] for sample in samples)
        peak_mem_percent = max(sample["system"]["memory_percent"] for sample in samples)
        peak_disk_percent = max(sample["system"]["disk_percent"] for sample in samples)
        peak_proc_cpu = max(sample["process"]["cpu_percent"] for sample in samples)
        peak_proc_memory = max(sample["process"]["memory_rss_mb"] for sample in samples)
        
        return {
            "peak_cpu_percent": peak_cpu,
            "peak_memory_percent": peak_mem_percent,
            "peak_disk_percent": peak_disk_percent,
            "peak_process_cpu": peak_proc_cpu,
            "peak_process_memory_mb": peak_proc_memory
        }
    
    def get_resource_trend(self, samples: List[Dict[str, Any]] = None) -> Dict[str, str]:
        """
        Get resource usage trend (increasing, decreasing, stable)
        """
        if samples is None:
            samples = self.monitoring_history
        
        if len(samples) < 2:
            return {"status": "insufficient_data"}
        
        # Calculate trends for key metrics
        cpu_values = [sample["system"]["cpu_percent"] for sample in samples]
        mem_values = [sample["system"]["memory_percent"] for sample in samples]
        proc_mem_values = [sample["process"]["memory_rss_mb"] for sample in samples]
        
        trends = {}
        
        # CPU trend
        if len(cpu_values) >= 2:
            avg_recent = sum(cpu_values[-min(5, len(cpu_values)):]) / min(5, len(cpu_values))
            avg_earlier = sum(cpu_values[:min(5, len(cpu_values))]) / min(5, len(cpu_values))
            trends["cpu"] = self._determine_trend(avg_earlier, avg_recent)
        
        # Memory trend
        if len(mem_values) >= 2:
            avg_recent = sum(mem_values[-min(5, len(mem_values)):]) / min(5, len(mem_values))
            avg_earlier = sum(mem_values[:min(5, len(mem_values))]) / min(5, len(mem_values))
            trends["memory"] = self._determine_trend(avg_earlier, avg_recent)
        
        # Process memory trend
        if len(proc_mem_values) >= 2:
            avg_recent = sum(proc_mem_values[-min(5, len(proc_mem_values)):]) / min(5, len(proc_mem_values))
            avg_earlier = sum(proc_mem_values[:min(5, len(proc_mem_values))]) / min(5, len(proc_mem_values))
            trends["process_memory"] = self._determine_trend(avg_earlier, avg_recent)
        
        return trends
    
    def _determine_trend(self, earlier_avg: float, recent_avg: float) -> str:
        """
        Determine trend based on comparison of averages
        """
        if recent_avg > earlier_avg * 1.1:  # 10% increase
            return "increasing"
        elif recent_avg < earlier_avg * 0.9:  # 10% decrease
            return "decreasing"
        else:
            return "stable"
    
    def get_resource_alerts(self, cpu_threshold: int = 90, 
                          memory_threshold: int = 85,
                          disk_threshold: int = 95) -> List[Dict[str, Any]]:
        """
        Get resource alerts based on thresholds
        Args:
            cpu_threshold: CPU usage threshold (%)
            memory_threshold: Memory usage threshold (%)
            disk_threshold: Disk usage threshold (%)
        Returns:
            List of alert conditions
        """
        current = self.get_current_usage()
        alerts = []
        
        if current["system"]["cpu_percent"] > cpu_threshold:
            alerts.append({
                "type": "HIGH_CPU",
                "metric": "cpu_percent",
                "current_value": current["system"]["cpu_percent"],
                "threshold": cpu_threshold,
                "severity": "WARNING" if current["system"]["cpu_percent"] < cpu_threshold + 10 else "CRITICAL"
            })
        
        if current["system"]["memory_percent"] > memory_threshold:
            alerts.append({
                "type": "HIGH_MEMORY",
                "metric": "memory_percent",
                "current_value": current["system"]["memory_percent"],
                "threshold": memory_threshold,
                "severity": "WARNING" if current["system"]["memory_percent"] < memory_threshold + 10 else "CRITICAL"
            })
        
        if current["system"]["disk_percent"] > disk_threshold:
            alerts.append({
                "type": "HIGH_DISK",
                "metric": "disk_percent",
                "current_value": current["system"]["disk_percent"],
                "threshold": disk_threshold,
                "severity": "WARNING" if current["system"]["disk_percent"] < disk_threshold + 5 else "CRITICAL"
            })
        
        return alerts
    
    def get_resource_recommendations(self) -> List[str]:
        """
        Get recommendations based on current resource usage
        """
        current = self.get_current_usage()
        recommendations = []
        
        if current["system"]["memory_percent"] > 85:
            recommendations.append("High memory usage detected - consider reducing parallel processing")
        
        if current["system"]["cpu_percent"] > 90:
            recommendations.append("High CPU usage detected - consider reducing worker count")
        
        if current["system"]["disk_percent"] > 90:
            recommendations.append("High disk usage detected - clean up temporary files")
        
        if current["process"]["memory_rss_mb"] > 1000:  # 1GB
            recommendations.append("Process memory usage high - consider memory-efficient processing")
        
        if not recommendations:
            recommendations.append("System resources are within normal ranges")
        
        return recommendations
    
    def get_monitoring_summary(self) -> Dict[str, Any]:
        """
        Get comprehensive monitoring summary
        """
        if not self.monitoring_history:
            return {"status": "no_data", "message": "Monitoring not started or no data collected"}
        
        current = self.get_current_usage()
        average = self.get_average_usage()
        peak = self.get_peak_usage()
        trend = self.get_resource_trend()
        alerts = self.get_resource_alerts()
        
        return {
            "current_usage": current,
            "average_usage": average,
            "peak_usage": peak,
            "trends": trend,
            "alerts": alerts,
            "recommendations": self.get_resource_recommendations(),
            "samples_collected": len(self.monitoring_history),
            "monitoring_duration": time.time() - getattr(self, 'monitoring_start_time', time.time()),
            "status": "monitoring_active" if self.is_monitoring else "monitoring_inactive"
        }
    
    def reset_monitoring(self):
        """
        Reset monitoring history
        """
        self.monitoring_history = []
        if self.is_monitoring:
            self.monitoring_start_time = time.time()
    
    def get_memory_usage_breakdown(self) -> Dict[str, Any]:
        """
        Get detailed memory usage breakdown
        """
        
        memory_info = psutil.virtual_memory()
        swap_info = psutil.swap_memory()
        
        # Get detailed process memory info
        process = psutil.Process()
        proc_memory = process.memory_info()
        proc_memory_full = process.memory_full_info()
        
        return {
            "system": {
                "total_memory_gb": memory_info.total / (1024**3),
                "available_memory_gb": memory_info.available / (1024**3),
                "used_memory_gb": memory_info.used / (1024**3),
                "memory_percent": memory_info.percent,
                "swap_total_gb": swap_info.total / (1024**3),
                "swap_used_gb": swap_info.used / (1024**3),
                "swap_percent": swap_info.percent
            },
            "process": {
                "rss_memory_mb": proc_memory.rss / (1024**2),  # Resident Set Size
                "vms_memory_mb": proc_memory.vms / (1024**2),  # Virtual Memory Size
                "shared_memory_mb": proc_memory.shared / (1024**2) if hasattr(proc_memory, 'shared') else 0,
                "text_memory_mb": proc_memory.text / (1024**2) if hasattr(proc_memory, 'text') else 0,
                "data_memory_mb": proc_memory.data / (1024**2) if hasattr(proc_memory, 'data') else 0,
                "uss_memory_mb": proc_memory_full.uss / (1024**2) if hasattr(proc_memory_full, 'uss') else 0,  # Unique Set Size
                "pss_memory_mb": proc_memory_full.pss / (1024**2) if hasattr(proc_memory_full, 'pss') else 0,  # Proportional Set Size
                "swap_memory_mb": proc_memory_full.swap / (1024**2) if hasattr(proc_memory_full, 'swap') else 0
            },
            "garbage_collector": {
                "objects_count": len(gc.get_objects()),
                "garbage_count": len(gc.garbage),
                "generation_counts": list(gc.get_count()),
                "generation_thresholds": list(gc.get_threshold())
            }
        }