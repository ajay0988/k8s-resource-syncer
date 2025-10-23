import time
import json
import logging
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException

# --- Configuration ---
SOURCE_CONFIGMAP_NAME = "source-configmap" # Updated to reflect ConfigMap
SOURCE_CONFIGMAP_NAMESPACE = "default"

# Define the list of target ConfigMaps to be synchronized. 
TARGET_CONFIGMAPS = [ # Updated name
    {"name": "config-1", "namespace": "dev"},
    {"name": "config-2", "namespace": "stage"},
]
# ---------------------

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def sync_configmap_data(source_data, core_v1):
    """
    Patches the target ConfigMaps with the data from the source ConfigMap.
    The data in ConfigMaps are plain string key/value pairs.
    """
    if not source_data:
        logging.warning("Source ConfigMap data is empty. Skipping synchronization.")
        return

    logging.info(f"Source ConfigMap data changed. Syncing to {len(TARGET_CONFIGMAPS)} target ConfigMaps...")
    
    # ConfigMap data is a dictionary of string-to-string.

    for target in TARGET_CONFIGMAPS: # Updated name
        target_name = target['name']
        target_ns = target['namespace']
        
        # Define the payload for the patch operation: only update the 'data' field.
        body = {"data": source_data} 
        
        try:
            # We use 'patch_namespaced_config_map' for ConfigMaps
            core_v1.patch_namespaced_config_map(
                name=target_name,
                namespace=target_ns,
                body=body
            )
            logging.info(f"Successfully synced data to ConfigMap: {target_ns}/{target_name}")

        except ApiException as e:
            # Error 404 (Not Found) is common if the target ConfigMap doesn't exist.
            if e.status == 404:
                logging.error(f"Target ConfigMap {target_ns}/{target_name} not found. Skipping.")
                continue
            
            # Log other API errors
            logging.error(f"Failed to patch ConfigMap {target_ns}/{target_name}: {e}")
        except Exception as e:
            logging.critical(f"An unexpected error occurred during sync: {e}")

def watch_source_configmap():
    """
    Main watch loop for monitoring the source ConfigMap.
    """
    try:
        # Load Kubernetes configuration using the Service Account token 
        # provided to the Pod (in-cluster configuration).
        config.load_incluster_config()

        logging.info("Kubernetes in-cluster configuration loaded successfully.")
        
        # Initialize the API client
        core_v1 = client.CoreV1Api()
        w = watch.Watch()
        
    except Exception as e:
        # This will fail if the script is run outside the cluster without a kubeconfig
        logging.critical(f"Failed to initialize Kubernetes client (Are you running in-cluster?): {e}")
        return

    resource_version = None
    backoff_time = 1 # Initial backoff time in seconds

    while True:
        try:
            # Watch for changes to only the source ConfigMap
            field_selector = f"metadata.name={SOURCE_CONFIGMAP_NAME}"

            logging.info(f"Starting watch stream for {SOURCE_CONFIGMAP_NAMESPACE}/{SOURCE_CONFIGMAP_NAME}...")
            
            # Use core_v1.list_namespaced_config_map for ConfigMaps
            for event in w.stream(
                func=core_v1.list_namespaced_config_map,
                namespace=SOURCE_CONFIGMAP_NAMESPACE,
                field_selector=field_selector,
                resource_version=resource_version,
                timeout_seconds=600 
            ):
                event_type = event['type']
                cm_obj = event['object']
                cm_name = cm_obj.metadata.name
                current_data = cm_obj.data # ConfigMaps use .data field (plaintext)
                
                # Update the resource version for the next watch stream
                resource_version = cm_obj.metadata.resource_version

                # We only sync on ADDED or MODIFIED events.
                if event_type in ["ADDED", "MODIFIED"]:
                    sync_configmap_data(current_data, core_v1) # Updated function call
                    # Reset backoff time on successful event processing
                    backoff_time = 1 
                
                elif event_type == "DELETED":
                    # --- CRITICAL: Do NOT sync or delete target CMs on source deletion. ---
                    logging.warning(f"Source ConfigMap {cm_name} was deleted. Target ConfigMaps will NOT be deleted.")
                    # The watch will continue until the source CM is ADDED back.
                
                else:
                    logging.debug(f"Ignored event type: {event_type}")

        except ApiException as e:
            # Catch errors related to RBAC (403 Forbidden) or resource gone (410 Gone)
            logging.error(f"Kubernetes API Exception in watch loop (Check RBAC!): {e}")
        except client.exceptions.MaxRetryError as e:
            logging.error(f"MaxRetryError (likely network issue): {e}")
        except Exception as e:
            logging.critical(f"An unexpected error occurred in the watch stream: {e}")
        
        # Exponential Backoff and Reconnect Logic
        logging.info(f"Watch stream disconnected. Retrying in {backoff_time} seconds...")
        time.sleep(backoff_time)
        backoff_time = min(backoff_time * 2, 60) # Double backoff, max 60 seconds

if __name__ == "__main__":
    logging.info("Starting ConfigMap Synchronization Controller...")
    watch_source_configmap() # Updated function call
