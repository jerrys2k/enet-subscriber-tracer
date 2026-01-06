import os
#!/usr/bin/env python3
"""
Sandvine Maestro Client
- Real-time subscriber session lookup
- Two-step: Get IP → Get Session Details
"""

import subprocess
import re
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class SandvineClient:
    """Client for Sandvine Maestro CLI queries"""
    
    HOST = "100.64.5.4"
    PORT = 42002
    USER = "admin"
    PASSWORD = os.environ.get("SANDVINE_PASSWORD", "password")
    
    # Full paths for gunicorn environment
    SSHPASS = "/usr/bin/sshpass"
    SSH = "/usr/bin/ssh"
    
    SSH_OPTIONS = [
        "-o", "StrictHostKeyChecking=no",
        "-o", "HostKeyAlgorithms=+ssh-rsa",
        "-o", "PubkeyAcceptedKeyTypes=+ssh-rsa",
        "-o", "ConnectTimeout=10",
        "-p", str(PORT)
    ]
    
    @classmethod
    def _run_command(cls, command):
        """Execute SSH command and return output"""
        try:
            full_cmd = [
                cls.SSHPASS, "-p", cls.PASSWORD,
                cls.SSH
            ] + cls.SSH_OPTIONS + [
                f"{cls.USER}@{cls.HOST}",
                command
            ]
            
            result = subprocess.run(
                full_cmd,
                capture_output=True,
                text=True,
                timeout=15
            )
            
            if result.returncode != 0:
                logger.error(f"Sandvine command failed: {result.stderr}")
                return None
            
            return result.stdout
            
        except subprocess.TimeoutExpired:
            logger.error("Sandvine command timed out")
            return None
        except Exception as e:
            logger.error(f"Sandvine error: {e}")
            return None
    
    @classmethod
    def _parse_ip(cls, output):
        """Extract IP address from subscriber lookup"""
        if not output:
            return None
        
        match = re.search(r'ip-address\s+(\d+\.\d+\.\d+\.\d+)', output)
        if match:
            return match.group(1)
        return None
    
    @classmethod
    def _parse_session(cls, output):
        """Parse session attributes into structured dict"""
        if not output:
            return None
        
        data = {
            "raw": output,
            "attributes": {}
        }
        
        # Parse session block
        session_match = re.search(r'session\s*\{[^}]*subscriber\s+(\d+)[^}]*session-id\s+([\d-]+)', output)
        if session_match:
            data["subscriber"] = session_match.group(1)
            data["session_id"] = session_match.group(2)
        
        # Parse IP
        ip_match = re.search(r'ip-address\s+(\d+\.\d+\.\d+\.\d+)', output)
        if ip_match:
            data["ip_address"] = ip_match.group(1)
        
        # Parse assigned time
        assigned_match = re.search(r'assigned\s+([\d\-T:+]+)', output)
        if assigned_match:
            data["session_start"] = assigned_match.group(1)
        
        # Parse all session-attributes
        attr_pattern = r'session-attributes\s*\{\s*name\s+(\S+)\s+value\s+(.+?)\s+type'
        for match in re.finditer(attr_pattern, output, re.DOTALL):
            name = match.group(1)
            value = match.group(2).strip()
            data["attributes"][name] = value
        
        # Extract key fields
        attrs = data["attributes"]
        data["enodeb_id"] = attrs.get("ENODEID")
        data["cell_id"] = attrs.get("CELLID")
        data["site_name"] = attrs.get("SITE_NAME")
        data["imsi"] = attrs.get("X3GPP_IMSI")
        data["imei"] = attrs.get("X3GPP_IMEISV")
        data["device_name"] = attrs.get("cus_device_name")
        data["device_type"] = attrs.get("cus_device_type")
        data["device_vendor"] = attrs.get("cus_vendor")
        data["is_roaming"] = attrs.get("isRoaming") == "true"
        data["rat_type_raw"] = attrs.get("X3GPP_RAT_Type")
        data["tac"] = attrs.get("TAC")
        
        # Decode RAT type
        rat_types = {
            "1": "UTRAN (3G)",
            "2": "GERAN (2G)", 
            "3": "WLAN",
            "6": "LTE (4G)",
            "7": "NR (5G)",
            "8": "NR (5G)"
        }
        data["rat_type"] = rat_types.get(str(attrs.get("X3GPP_RAT_Type", "")), "Unknown")
        
        # Calculate session duration
        session_create = attrs.get("SessionCreateTime")
        if session_create:
            try:
                from datetime import datetime
                create_ms = int(session_create)
                create_time = datetime.fromtimestamp(create_ms / 1000)
                data["session_start_formatted"] = create_time.strftime("%Y-%m-%d %H:%M:%S")
                duration = datetime.now() - create_time
                hours, remainder = divmod(int(duration.total_seconds()), 3600)
                minutes, seconds = divmod(remainder, 60)
                if hours > 0:
                    data["session_duration"] = f"{hours}h {minutes}m"
                else:
                    data["session_duration"] = f"{minutes}m {seconds}s"
            except:
                data["session_start_formatted"] = data.get("session_start")
                data["session_duration"] = "Unknown"
        
        return data
    
    @classmethod
    def get_live_session(cls, msisdn):
        """
        Get real-time session data for MSISDN
        Returns: dict with session details or {"error": "..."} 
        """
        # Normalize MSISDN
        msisdn = msisdn.replace(" ", "").replace("-", "")
        if not msisdn.startswith("592") and len(msisdn) == 7:
            msisdn = f"592{msisdn}"
        
        logger.info(f"Sandvine lookup for {msisdn}")
        
        # Step 1: Get subscriber IP
        cmd1 = f"show service subscriber-management get-attribute-details name {msisdn} attribute-view Profile"
        output1 = cls._run_command(cmd1)
        
        if not output1:
            return {"error": "Sandvine connection failed"}
        
        if "not found" in output1.lower():
            return {"error": "Subscriber not found"}
        
        ip = cls._parse_ip(output1)
        if not ip:
            return {"error": "No active session (subscriber offline)"}
        
        # Step 2: Get session details
        cmd2 = f"show service session-management get-ip-details ip {ip}"
        output2 = cls._run_command(cmd2)
        
        if not output2:
            return {"error": "Session lookup failed"}
        
        # Parse and return
        session = cls._parse_session(output2)
        if session:
            session["source"] = "sandvine_live"
            session["lookup_time"] = datetime.now().isoformat()
            logger.info(f"Sandvine: Found {msisdn} at {session.get('site_name')} (eNodeB {session.get('enodeb_id')})")
            return session
        
        return {"error": "Failed to parse session data"}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = SandvineClient.get_live_session("5927133684")
    print("=" * 50)
    if "error" in result:
        print(f"Error: {result['error']}")
    else:
        print(f"MSISDN: {result.get('subscriber')}")
        print(f"IP: {result.get('ip_address')}")
        print(f"Site: {result.get('site_name')}")
        print(f"eNodeB: {result.get('enodeb_id')}")
        print(f"Cell: {result.get('cell_id')}")
        print(f"IMSI: {result.get('imsi')}")
        print(f"IMEI: {result.get('imei')}")
        print(f"Device: {result.get('device_vendor')} {result.get('device_name')}")
