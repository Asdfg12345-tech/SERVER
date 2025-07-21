import xml.etree.ElementTree as ET
from opcua import Server, ua
import time
import os
import sys
import argparse
from pathlib import Path
import threading
import logging
from datetime import datetime, timezone

class OPCUAServer:
    def __init__(self, config_path=None):
        logging.basicConfig(level=logging.ERROR)
        logger = logging.getLogger('opcua')
        logger.setLevel(logging.ERROR)
        
        self.config_path = self._resolve_config_path(config_path)
        self.server = None
        self.config = None
        self.objects = None
        self.running = False
        self.watcher_thread = None
        self.keepalive_thread = None
        self.nodes = {}  # Track nodes for value updates
        self._dummy_var = None  # Keep-alive dummy variable

    def _resolve_config_path(self, config_arg=None):
        if config_arg:
            config_arg = os.path.abspath(config_arg)
            if os.path.exists(config_arg):
                return config_arg
            raise FileNotFoundError(f"Config file not found at: {config_arg}")

        # Determine base paths based on execution context
        if getattr(sys, 'frozen', False):
            # Running as executable - look in parent/configs
            base_path = Path(sys.executable).parent.parent
            config_dir = base_path / "configs"
        else:
            # Running as script - look in parent/configs
            base_path = Path(__file__).parent.parent
            config_dir = base_path / "configs"

        # First: Check if configs directory exists
        if not config_dir.exists():
            error_msg = [
                "ERROR: Configs directory not found.",
                f"Expected location: {config_dir}",
                "Create 'configs' directory and place XML files there."
            ]
            raise FileNotFoundError('\n'.join(error_msg))

        # Second: Look for any XML file in configs directory
        xml_files = list(config_dir.glob("*.xml"))
        if xml_files:
            return str(xml_files[0])  # Return first found XML

        # Error if no configs found
        error_msg = [
            "ERROR: No configuration files found in configs directory.",
            "",
            "Possible solutions:",
            f"1. Place ANY_NAME.xml in: {config_dir}",
            "2. Specify custom path with --config argument",
            "",
            "Example:",
            f"  {sys.argv[0]} --config configs/your_config.xml"
        ]
        raise FileNotFoundError('\n'.join(error_msg))

    def parse_config(self):
        try:
            # Ensure absolute path
            if not os.path.isabs(self.config_path):
                self.config_path = os.path.abspath(self.config_path)
                
            print(f"Loading config from: {self.config_path}")
            tree = ET.parse(self.config_path)
            root = tree.getroot()
            
            config_node = root.find('Config')
            if config_node is None:
                raise ET.ParseError("Missing <Config> section in XML")
                
            server_config = {
                'host': config_node.attrib['host'],
                'port': int(config_node.attrib['port']),
                'securityMode': config_node.attrib['securityMode']
            }

            def parse_node(xml_node):
                node_info = {'children': {}}
                
                for child in xml_node:
                    if 'id' in child.attrib and 'type' in child.attrib:
                        base_name = child.attrib['id']
                        signal_info = {
                            'type': 'signal',
                            'id': child.attrib['id'],
                            'data_type': child.attrib['type']
                        }
                        child_info_to_store = signal_info
                    else:
                        base_name = child.tag
                        folder_info = parse_node(child)
                        folder_info['type'] = 'folder'
                        child_info_to_store = folder_info

                    child_name = base_name
                    if child_name in node_info['children']:
                        suffix = 1
                        while f"{child_name}_{suffix}" in node_info['children']:
                            suffix += 1
                        child_name = f"{child_name}_{suffix}"
                    
                    node_info['children'][child_name] = child_info_to_store
                return node_info

            objects = {}
            objects_node = root.find('Objects')
            if objects_node is None:
                raise ET.ParseError("Missing <Objects> section in XML")
                
            for obj in objects_node:
                obj_id = obj.tag
                objects[obj_id] = parse_node(obj)
                objects[obj_id]['type'] = 'folder'
            
            return server_config, objects
        except Exception as e:
            print(f"Config Error: {str(e)}")
            raise

    def create_server(self):
        self.server = Server()
        app_uri = "urn:your:opcua:server"
        self.server.set_application_uri(app_uri)
        self.server.set_server_name("OPC UA XML Server")
        
        app_desc = ua.ApplicationDescription()
        app_desc.ApplicationUri = app_uri
        app_desc.ProductUri = "urn:your:product"
        app_desc.ApplicationName = ua.LocalizedText("OPC UA XML Server")
        app_desc.ApplicationType = ua.ApplicationType.Server
        self.server.application_description = app_desc
        
        endpoint = f"opc.tcp://{self.config['host']}:{self.config['port']}"
        self.server.set_endpoint(endpoint)
        self.server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
        
        # Try to configure server for better subscription handling
        try:
            # Try to set parameters using available methods
            if hasattr(self.server, 'set_min_publishing_interval'):
                self.server.set_min_publishing_interval(10.0)
                print("Set min publishing interval via server method")
            elif hasattr(self.server.iserver.aspace, 'set_min_publishing_interval'):
                self.server.iserver.aspace.set_min_publishing_interval(10.0)
                print("Set min publishing interval via AddressSpace method")
            else:
                print("Using keep-alive fallback only")
        except Exception as e:
            print(f"Could not configure server parameters: {str(e)}")
            print("Using keep-alive fallback only")
        
        idx = self.server.register_namespace("urn:your:machine:namespace")
        
        # Create a keep-alive dummy variable
        root = self.server.nodes.objects
        self._dummy_var = root.add_variable(idx, "KeepAliveDummy", False)
        self._dummy_var.set_writable(False)
        
        # Build node structure
        for obj_id, obj_info in self.objects.items():
            obj_ua_node = root.add_object(idx, obj_id)
            self._add_nodes_recursive(obj_ua_node, obj_info, idx)

    def _update_node_value(self, node, value, data_type):
        """Helper to update node value with proper timestamps"""
        variant = ua.Variant(value, self._get_ua_type(data_type))
        now = datetime.now(timezone.utc)
        dv = ua.DataValue(variant)
        dv.SourceTimestamp = now
        dv.ServerTimestamp = now
        node.set_data_value(dv)

    def _add_nodes_recursive(self, parent_ua_node, node_info, idx):
        if 'children' in node_info:
            for child_name, child_info in node_info['children'].items():
                if child_info['type'] == 'signal':
                    # Create initial value
                    init_value = self._get_initial_value(child_info['data_type'])
                    
                    # Create variable
                    var = parent_ua_node.add_variable(
                        idx, 
                        child_name, 
                        init_value, 
                        self._get_ua_type(child_info['data_type'])
                    )
                    
                    # Set initial value with proper timestamps
                    self._update_node_value(var, init_value, child_info['data_type'])
                    
                    # Store node reference
                    self.nodes[var.nodeid.to_string()] = {
                        'node': var,
                        'data_type': child_info['data_type']
                    }

                    var.set_writable()  # Allow writes to this variable

                else:
                    child_ua_node = parent_ua_node.add_object(idx, child_name)
                    self._add_nodes_recursive(child_ua_node, child_info, idx)

    def _get_ua_type(self, signal_type):
        type_map = {
            'Boolean': ua.VariantType.Boolean,
            'Int16': ua.VariantType.Int16,
            'UInt16': ua.VariantType.UInt16,
            'Int32': ua.VariantType.Int32,
            'UInt32': ua.VariantType.UInt32,
            'Float': ua.VariantType.Float,
            'Double': ua.VariantType.Double,
            'String': ua.VariantType.String,
            'DateTime': ua.VariantType.DateTime
        }
        return type_map.get(signal_type, ua.VariantType.String)

    def _get_initial_value(self, signal_type):
        value_map = {
            'Boolean': False,
            'Int16': 0,
            'UInt16': 0,
            'Int32': 0,
            'UInt32': 0,
            'Float': 0.0,
            'Double': 0.0,
            'String': "Initial String",
            'DateTime': datetime.now(timezone.utc)
        }
        return value_map.get(signal_type, "")

    def reload_config(self):
        try:
            new_config, new_objects = self.parse_config()
            if new_config != self.config or new_objects != self.objects:
                print("Configuration changed - updating server...")
                self.config, self.objects = new_config, new_objects
                if self.server:
                    self.server.stop()
                self.nodes = {}
                self.create_server()
                self.server.start()
        except Exception as e:
            print(f"Reload failed: {str(e)} (using previous config)")

    def watch_config_changes(self, interval=5):
        last_mtime = os.path.getmtime(self.config_path)
        while self.running:
            try:
                current_mtime = os.path.getmtime(self.config_path)
                if current_mtime > last_mtime:
                    last_mtime = current_mtime
                    self.reload_config()
            except Exception as e:
                print(f"Config watch error: {str(e)}")
            time.sleep(interval)
            
    def keep_alive(self):
        """Periodically toggle dummy variable to keep subscriptions alive"""
        while self.running:
            try:
                if self._dummy_var:
                    current_val = self._dummy_var.get_value()
                    self._update_node_value(
                        self._dummy_var, 
                        not current_val, 
                        "Boolean"
                    )
                time.sleep(0.5)
            except Exception as e:
                print(f"Keep-alive error: {str(e)}")
                time.sleep(1)

    def monitor_node_changes(self, interval=0.5):
        # Periodically check for value changes and update timestamps.
        last_values = {nid: node['node'].get_value() for nid, node in self.nodes.items()}
        while self.running:
            for nid, node_info in self.nodes.items():
                node = node_info['node']
                data_type = node_info['data_type']
            try:
                current_value = node.get_value()
                if current_value != last_values[nid]:
                    self._update_node_value(node, current_value, data_type)
                    last_values[nid] = current_value
            except Exception as e:
                print(f"Monitor error on {nid}: {e}")
        time.sleep(interval)
            

    def start(self):
        try:
            self.config, self.objects = self.parse_config()
            self.create_server()
            self.server.start()
            
            print(f"Server running at opc.tcp://{self.config['host']}:{self.config['port']}")
            print("Press Ctrl+C to stop...")
            
            self.running = True

            self.monitor_thread = threading.Thread(
                target=self.monitor_node_changes,
                daemon=True
            )
            self.monitor_thread.start()

            
            # Start config watcher thread
            self.watcher_thread = threading.Thread(
                target=self.watch_config_changes,
                daemon=True
            )
            self.watcher_thread.start()
            
            # Start keep-alive thread
            self.keepalive_thread = threading.Thread(
                target=self.keep_alive,
                daemon=True
            )
            self.keepalive_thread.start()
            
            while self.running:
                time.sleep(1)
                
        except KeyboardInterrupt:
            print("\nShutting down...")
        except Exception as e:
            print(f"Server error: {str(e)}")
            self.server = None
            raise
        finally:
            self.stop()
    
    def stop(self):
        self.running = False
        if self.watcher_thread:
            self.watcher_thread.join(timeout=1)
        if self.keepalive_thread:
            self.keepalive_thread.join(timeout=1)
        if hasattr(self, 'monitor_thread') and self.monitor_thread:
            self.monitor_thread.join(timeout=1)
        if self.server:
            self.server.stop()

def main():
    parser = argparse.ArgumentParser(description="OPC UA Server with External Config")
    parser.add_argument(
        '--config',
        help='Path to configuration XML file (default: first XML in configs/ directory)',
        default=None
    )
    args = parser.parse_args()

    try:
        server = OPCUAServer(args.config)
        server.start()
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        print("\nPress Enter to exit...")
        input()
        sys.exit(1)

if __name__ == "__main__":
    main()