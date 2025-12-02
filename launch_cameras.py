"""
Multi-Camera Frame Extractor Launcher - High Quality Version
Launches multiple RTSP extractors for different cameras with higher FPS
"""

import subprocess
import sys
import time
from multiprocessing import Process

CAMERAS = [
    {
        "camera_id": "CAM001",
        "rtsp_url": "rtsp://testaicam:testaicam@192.168.1.159:554/stream1",
        "fps": 5 ,   
    }
]

KAFKA_SERVERS = ['localhost:9092']
TOPIC = 'camera-frames'


def run_extractor(camera_config):
    """Run frame extractor for a single camera"""
    from rtsp_to_kafka import RTSPToKafka
    
    print(f"Starting extractor for {camera_config['camera_id']}")
    
    extractor = RTSPToKafka(
        rtsp_url=camera_config['rtsp_url'],
        kafka_servers=KAFKA_SERVERS,
        topic=TOPIC,
        camera_id=camera_config['camera_id'],
        fps=camera_config['fps']
    )
    
    extractor.connect()
    extractor.run()


def main():
    processes = []
    
    try:
        # Launch extractor for each camera
        for camera in CAMERAS:
            p = Process(target=run_extractor, args=(camera,))
            p.start()
            processes.append(p)
            time.sleep(1)  # Stagger startup
        
        print(f"Launched {len(processes)} camera extractors")
        print(f"Settings: {CAMERAS[0]['fps']} FPS, 75% resolution, Quality 80")
        print("Press Ctrl+C to stop all extractors")
        
        # Wait for all processes
        for p in processes:
            p.join()
            
    except KeyboardInterrupt:
        print("\nStopping all extractors...")
        for p in processes:
            p.terminate()
        
        # Wait for cleanup
        for p in processes:
            p.join(timeout=5)
        
        print("All extractors stopped")


if __name__ == "__main__":
    main()