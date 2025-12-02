import cv2
import base64
import json
import time
from kafka import KafkaProducer
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RTSPToKafka:
    def __init__(self, rtsp_url, kafka_servers, topic, camera_id, fps=10):
        """
        Args:
            rtsp_url: RTSP stream URL
            kafka_servers: List of Kafka broker addresses ['localhost:9092']
            topic: Kafka topic name
            camera_id: Unique camera identifier
            fps: Target frames per second
        """
        
        self.rtsp_url = rtsp_url
        self.topic = topic
        self.camera_id = camera_id
        self.target_fps = fps
        self.frame_interval = 1.0 / fps
        
        # Initialize Kafka Producer
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            compression_type='gzip',  # Compress for bandwidth
            max_request_size=10485760,  # 10MB for large frames
            
            linger_ms=0,                 # ⭐ Send immediately (was 10)
            batch_size=32768,            # ⭐ 32KB batches (was 16KB)
            buffer_memory=67108864,      # 64MB buffer
            request_timeout_ms=30000,    # 30 seconds
            delivery_timeout_ms=120000,  # 2 minutes
            acks=1,                      # Wait for leader only
            retries=3,
        )
        
        # Initialize video capture
        self.cap = None
        self.running = False
        
    def connect(self):
        """Connect to RTSP stream"""
        import os
        
        logger.info(f"Connecting to RTSP: {self.rtsp_url}")
        
        
        os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = "rtsp_transport;tcp"
        
        self.cap = cv2.VideoCapture(self.rtsp_url, cv2.CAP_FFMPEG)
        
        # เพิ่ม timeout
        self.cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, 60000)
        self.cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, 60000)
        self.cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        
        if not self.cap.isOpened():
            raise Exception("Failed to open RTSP stream")
        
        logger.info("RTSP connection established")
        return True
    
    def encode_frame(self, frame, quality=80):
        """
        Encode frame to JPEG base64 with better quality
        
        ⭐ IMPROVED VERSION:
        - Resize only 25% instead of 50% (better quality)
        - Higher JPEG quality (80 instead of 60)
        - Option to skip resize for high-quality mode
        """
        
        # ⭐ Option 1: Better resize (75% of original)
        height, width = frame.shape[:2]
        new_width = int(width * 0.75)   # 1920 → 1440 (was 960)
        new_height = int(height * 0.75)  # 1080 → 810 (was 540)
        frame = cv2.resize(frame, (new_width, new_height), interpolation=cv2.INTER_LANCZOS4)
        
        # ⭐ Option 2: Full resolution (uncomment to use)
        # Comment out the resize above and uncomment this:
        # No resize - use original resolution
        # (Only use if you have good bandwidth)
        
        # ⭐ Higher quality encoding
        encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 80]  # 80 instead of 60
        _, buffer = cv2.imencode('.jpg', frame, encode_param)
        
        jpg_as_text = base64.b64encode(buffer).decode('utf-8')
        return jpg_as_text
    
    def publish_frame(self, frame_data):
        """Publish frame to Kafka"""
        try:
            future = self.producer.send(self.topic, value=frame_data)
            # Block for 'synchronous' sends (optional)
            # future.get(timeout=1)
        except Exception as e:
            logger.error(f"Failed to publish frame: {e}")
    
    def run(self):
        """Main loop - capture and publish frames"""
        self.running = True
        last_frame_time = 0
        frame_count = 0
        
        logger.info(f"Starting frame extraction at {self.target_fps} FPS")
        logger.info(f"Resolution: 75% of original (better quality)")
        logger.info(f"JPEG Quality: 80 (higher quality)")
        
        try:
            while self.running:
                current_time = time.time()
                
                # FPS control
                if current_time - last_frame_time < self.frame_interval:
                    time.sleep(0.001)  # Small sleep to prevent CPU spinning
                    continue
                
                ret, frame = self.cap.read()
                
                if not ret:
                    logger.warning("Failed to read frame, reconnecting...")
                    self.reconnect()
                    continue
                
                # Encode frame
                frame_b64 = self.encode_frame(frame)
                
                # Prepare message
                message = {
                    "camera_id": self.camera_id,
                    "image_b64": frame_b64,
                    "timestamp": int(current_time * 1000),  # milliseconds
                    "frame_count": frame_count,
                }
                
                # Publish to Kafka
                self.publish_frame(message)
                
                last_frame_time = current_time
                frame_count += 1
                
                if frame_count % 100 == 0:
                    logger.info(f"Published {frame_count} frames")
                    
        except KeyboardInterrupt:
            logger.info("Stopping...")
        finally:
            self.cleanup()
    
    def reconnect(self):
        """Reconnect to RTSP stream"""
        logger.info("Attempting to reconnect...")
        if self.cap:
            self.cap.release()
        time.sleep(2)
        self.connect()
    
    def cleanup(self):
        """Cleanup resources"""
        logger.info("Cleaning up...")
        self.running = False
        if self.cap:
            self.cap.release()
        if self.producer:
            self.producer.flush()
            self.producer.close()
        logger.info("Cleanup complete")


def main():
    # Configuration
    RTSP_URL = "rtsp://testaicam:testaicam@192.168.1.159:554/stream1"
    KAFKA_SERVERS = ['localhost:9092']
    TOPIC = "camera-frames"
    CAMERA_ID = "CAM001"
    FPS = 5
    
    # Create and run extractor
    extractor = RTSPToKafka(
        rtsp_url=RTSP_URL,
        kafka_servers=KAFKA_SERVERS,
        topic=TOPIC,
        camera_id=CAMERA_ID,
        fps=FPS
    )
    
    extractor.connect()
    extractor.run()


if __name__ == "__main__":
    main()