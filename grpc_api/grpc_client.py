import argparse
import json
import time
import pyaudio
import wave
import grpc
import requests
import transcription_service_pb2_grpc as pb2_grpc
import transcription_service_pb2 as pb2
from utils import heconstants


class AudioCapture:
    def __init__(self, source=None, format=pyaudio.paInt16, channels=1, rate=16000, chunk_size=1024):
        """
        Initialize the audio capture.
        :param source: Path to a local audio file or None to capture from microphone.
        :param format: The format of the audio (pyaudio format constant).
        :param channels: Number of audio channels.
        :param rate: Sampling rate.
        :param chunk_size: Size of each audio chunk to read.
        """
        self.source = source
        self.format = format
        self.channels = channels
        self.rate = rate
        self.chunk_size = chunk_size
        self.audio_interface = pyaudio.PyAudio()

    def capture_chunks(self):
        """Generator that yields audio chunks from the microphone or a local file."""
        if self.source is None:
            # Capture from microphone
            stream = self.audio_interface.open(format=self.format, channels=self.channels, rate=self.rate, input=True,
                                               frames_per_buffer=self.chunk_size)
            try:
                while True:
                    data = stream.read(self.chunk_size, exception_on_overflow=False)
                    yield data
            finally:
                stream.stop_stream()
                stream.close()
                self.audio_interface.terminate()
        else:
            # Read from local file and simulate real-time streaming
            with wave.open(self.source, 'rb') as wf:
                data = wf.readframes(self.chunk_size)
                while data:
                    start_time = time.time()
                    yield data
                    data = wf.readframes(self.chunk_size)
                    # Calculate the real-time duration of the chunk and sleep
                    duration = len(data) / (self.rate * self.channels * wf.getsampwidth())
                    time.sleep(max(0, duration - (time.time() - start_time)))


def stream_audio(server_address='localhost:50051', audio_source=None):
    """
    Streams audio to the transcription server, from either a microphone or a local audio file.
    :param server_address: Address of the gRPC server.
    :param audio_source: Path to a local audio file or None to capture from microphone.
    """
    channel = grpc.insecure_channel(server_address)
    stub = pb2_grpc.TranscriptionServiceStub(channel)

    audio_capture = AudioCapture(source=audio_source)

    def generate_audio_chunks():
        for chunk in audio_capture.capture_chunks():
            yield pb2.AudioChunk(audio_content=chunk)

    last_preds_sent_at = time.time()
    last_trans_sent_at = time.time()
    responses = stub.StreamTranscription(generate_audio_chunks())
    for response in responses:
        print("Quick CC:", response.cc)
        conversation_id = response.conversation_id
        latest_ai_preds_resp = None
        if time.time() - last_preds_sent_at >= 10:
            ai_preds_resp = requests.get(
                heconstants.SYNC_SERVER + f"/history?conversation_id={conversation_id}"
            )
            if ai_preds_resp.status_code == 200:
                latest_ai_preds_resp = json.loads(ai_preds_resp.text)
            last_preds_sent_at = time.time()

        elif time.time() - last_trans_sent_at >= 8:
            ai_preds_resp = requests.get(
                heconstants.SYNC_SERVER + f"/history?conversation_id={conversation_id}&only_transcribe=True"
            )
            if ai_preds_resp.status_code == 200:
                latest_ai_preds_resp = json.loads(ai_preds_resp.text)
            last_trans_sent_at = time.time()
        print(f"AI Preds: {latest_ai_preds_resp}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default=None,
                        help="Specify the source of the audio. The default is the microphone. To read from a file, "
                             "please provide the file path.", type=str)
    args = parser.parse_args()
    audio_source = args.source
    stream_audio(audio_source=audio_source)
